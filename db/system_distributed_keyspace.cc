/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "db/system_distributed_keyspace.hh"

#include "cql3/untyped_result_set.hh"
#include "database.hh"
#include "db/consistency_level_type.hh"
#include "db/system_keyspace.hh"
#include "schema_builder.hh"
#include "timeout_config.hh"
#include "types.hh"
#include "types/tuple.hh"
#include "types/set.hh"
#include "cdc/generation.hh"
#include "cql3/query_processor.hh"

#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>

#include <boost/range/adaptor/transformed.hpp>

#include <optional>
#include <vector>
#include <optional>

namespace db {

thread_local data_type cdc_streams_set_type = set_type_impl::get_instance(bytes_type, false);

schema_ptr view_build_status() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::VIEW_BUILD_STATUS);
        return schema_builder(system_distributed_keyspace::NAME, system_distributed_keyspace::VIEW_BUILD_STATUS, std::make_optional(id))
                .with_column("keyspace_name", utf8_type, column_kind::partition_key)
                .with_column("view_name", utf8_type, column_kind::partition_key)
                .with_column("host_id", uuid_type, column_kind::clustering_key)
                .with_column("status", utf8_type)
                .with_version(system_keyspace::generate_schema_version(id))
                .build();
    }();
    return schema;
}

/* A user-facing table providing identifiers of the streams used in CDC generations. */
schema_ptr cdc_desc() {
    thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_DESC);
        return schema_builder(system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_DESC, {id})
                /* The timestamp of this CDC generation. */
                .with_column("time", timestamp_type, column_kind::partition_key)
                /* The set of stream identifiers used in this CDC generation. */
                .with_column("streams", cdc_streams_set_type)
                /* Expiration time of this CDC generation (or null if not expired). */
                .with_column("expired", timestamp_type)
                .with_version(system_keyspace::generate_schema_version(id))
                .build();
    }();
    return schema;
}

static std::vector<schema_ptr> all_tables() {
    return {
        view_build_status(),
        cdc_desc(),
    };
}

system_distributed_keyspace::system_distributed_keyspace(cql3::query_processor& qp, service::migration_manager& mm)
        : _qp(qp)
        , _mm(mm) {
}

future<> system_distributed_keyspace::start() {
    if (this_shard_id() != 0) {
        return make_ready_future<>();
    }

    static auto ignore_existing = [] (seastar::noncopyable_function<future<>()> func) {
        return futurize_invoke(std::move(func)).handle_exception_type([] (exceptions::already_exists_exception& ignored) { });
    };

    // We use min_timestamp so that the default keyspace metadata will lose with any manual adjustments.
    // See issue #2129.
    return ignore_existing([this] {
        auto ksm = keyspace_metadata::new_keyspace(
                NAME,
                "org.apache.cassandra.locator.SimpleStrategy",
                {{"replication_factor", "3"}},
                true);
        return _mm.announce_new_keyspace(ksm, api::min_timestamp, false);
    }).then([this] {
        return do_with(all_tables(), [this] (std::vector<schema_ptr>& tables) {
            return do_for_each(tables, [this] (schema_ptr table) {
                return ignore_existing([this, table = std::move(table)] {
                    return _mm.announce_new_column_family(std::move(table), api::min_timestamp, false);
                });
            });
        });
    });
}

future<> system_distributed_keyspace::stop() {
    return make_ready_future<>();
}

static const timeout_config internal_distributed_timeout_config = [] {
    using namespace std::chrono_literals;
    const auto t = 10s;
    return timeout_config{ t, t, t, t, t, t, t };
}();

future<std::unordered_map<utils::UUID, sstring>> system_distributed_keyspace::view_status(sstring ks_name, sstring view_name) const {
    return _qp.execute_internal(
            format("SELECT host_id, status FROM {}.{} WHERE keyspace_name = ? AND view_name = ?", NAME, VIEW_BUILD_STATUS),
            db::consistency_level::ONE,
            internal_distributed_timeout_config,
            { std::move(ks_name), std::move(view_name) },
            false).then([this] (::shared_ptr<cql3::untyped_result_set> cql_result) {
        return boost::copy_range<std::unordered_map<utils::UUID, sstring>>(*cql_result
                | boost::adaptors::transformed([] (const cql3::untyped_result_set::row& row) {
                    auto host_id = row.get_as<utils::UUID>("host_id");
                    auto status = row.get_as<sstring>("status");
                    return std::pair(std::move(host_id), std::move(status));
                }));
    });
}

future<> system_distributed_keyspace::start_view_build(sstring ks_name, sstring view_name) const {
    return db::system_keyspace::get_local_host_id().then([this, ks_name = std::move(ks_name), view_name = std::move(view_name)] (utils::UUID host_id) {
        return _qp.execute_internal(
                format("INSERT INTO {}.{} (keyspace_name, view_name, host_id, status) VALUES (?, ?, ?, ?)", NAME, VIEW_BUILD_STATUS),
                db::consistency_level::ONE,
                internal_distributed_timeout_config,
                { std::move(ks_name), std::move(view_name), std::move(host_id), "STARTED" },
                false).discard_result();
    });
}

future<> system_distributed_keyspace::finish_view_build(sstring ks_name, sstring view_name) const {
    return db::system_keyspace::get_local_host_id().then([this, ks_name = std::move(ks_name), view_name = std::move(view_name)] (utils::UUID host_id) {
        return _qp.execute_internal(
                format("UPDATE {}.{} SET status = ? WHERE keyspace_name = ? AND view_name = ? AND host_id = ?", NAME, VIEW_BUILD_STATUS),
                db::consistency_level::ONE,
                internal_distributed_timeout_config,
                { "SUCCESS", std::move(ks_name), std::move(view_name), std::move(host_id) },
                false).discard_result();
    });
}

future<> system_distributed_keyspace::remove_view(sstring ks_name, sstring view_name) const {
    return _qp.execute_internal(
            format("DELETE FROM {}.{} WHERE keyspace_name = ? AND view_name = ?", NAME, VIEW_BUILD_STATUS),
            db::consistency_level::ONE,
            internal_distributed_timeout_config,
            { std::move(ks_name), std::move(view_name) },
            false).discard_result();
}

/* We want to make sure that writes/reads to/from cdc_topology_description and cdc_description
 * are consistent: a read following an acknowledged write to the same partition should contact
 * at least one of the replicas that the write contacted.
 * Normally we would achieve that by always using CL = QUORUM,
 * but there's one special case when that's impossible: a single-node cluster. In that case we'll
 * use CL = ONE for writing the data, which will do the right thing -- saving the data in the only
 * possible replica. Until another node joins, reads will also use CL = ONE, retrieving the data
 * from the only existing replica.
 *
 * There is one case where queries wouldn't see the read: if we extend the single-node cluster
 * with two nodes without bootstrapping (so the data won't be streamed to new replicas),
 * and the admin forgets to run repair. Then QUORUM reads might contact only the two new nodes
 * and miss the written entry.
 *
 * Fortunately (aside from the fact that nodes shouldn't be joined without bootstrapping),
 * after the second node joins, it will propose a new CDC generation, so the old entry
 * that was written with CL=ONE won't be used by the cluster anymore. All nodes other than
 * the first one use QUORUM to make the write.
 *
 * And even if the old entry was still needed for some reason, by the time the third node joins,
 * the second node would have already fixed our issue by running read repair on the old entry.
 */
static db::consistency_level quorum_if_many(size_t num_token_owners) {
    return num_token_owners > 1 ? db::consistency_level::QUORUM : db::consistency_level::ONE;
}

static set_type_impl::native_type prepare_cdc_streams(const std::vector<cdc::stream_id>& streams) {
    set_type_impl::native_type ret;
    for (auto& s: streams) {
        ret.push_back(data_value(s.to_bytes()));
    }
    return ret;
}

future<>
system_distributed_keyspace::create_cdc_desc(
        db_clock::time_point time,
        const std::vector<cdc::stream_id>& streams,
        context ctx) {
    return _qp.execute_internal(
            format("INSERT INTO {}.{} (time, streams) VALUES (?,?)", NAME, CDC_DESC),
            quorum_if_many(ctx.num_token_owners),
            internal_distributed_timeout_config,
            { time, make_set_value(cdc_streams_set_type, prepare_cdc_streams(streams)) },
            false).discard_result();
}

future<>
system_distributed_keyspace::expire_cdc_desc(
        db_clock::time_point streams_ts,
        db_clock::time_point expiration_time,
        context ctx) {
    return _qp.execute_internal(
            format("UPDATE {}.{} SET expired = ? WHERE time = ?", NAME, CDC_DESC),
            quorum_if_many(ctx.num_token_owners),
            internal_distributed_timeout_config,
            { expiration_time, streams_ts },
            false).discard_result();
}

future<bool>
system_distributed_keyspace::cdc_desc_exists(
        db_clock::time_point streams_ts,
        context ctx) {
    return _qp.execute_internal(
            format("SELECT time FROM {}.{} WHERE time = ?", NAME, CDC_DESC),
            quorum_if_many(ctx.num_token_owners),
            internal_distributed_timeout_config,
            { streams_ts },
            false
    ).then([] (::shared_ptr<cql3::untyped_result_set> cql_result) -> bool {
        return !cql_result->empty() && cql_result->one().has("time");
    });
}

}
