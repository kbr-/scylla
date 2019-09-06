/*
 * Copyright (C) 2019 ScyllaDB
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

#include <utility>

#include "cdc/cdc.hh"
#include "bytes.hh"
#include "database.hh"
#include "db/config.hh"
#include "dht/murmur3_partitioner.hh"
#include "partition_slice_builder.hh"
#include "schema.hh"
#include "schema_builder.hh"
#include "service/migration_manager.hh"
#include "service/storage_service.hh"

using locator::snitch_ptr;
using locator::token_metadata;
using locator::topology;
using seastar::sstring;
using service::migration_manager;
using service::storage_proxy;

namespace cdc {

sstring log_name(const sstring& table_name) {
    static constexpr auto cdc_log_suffix = "_scylla_cdc_log";
    return table_name + cdc_log_suffix;
}

sstring desc_name(const sstring& table_name) {
    static constexpr auto cdc_desc_suffix = "_scylla_cdc_desc";
    return table_name + cdc_desc_suffix;
}

static future<>
remove_log(db_context ctx, const sstring& ks_name, const sstring& table_name) {
    try {
        return ctx._migration_manager.announce_column_family_drop(
                ks_name, log_name(table_name), false);
    } catch (exceptions::configuration_exception& e) {
        // It's fine if the table does not exist.
        return make_ready_future<>();
    }
}

static future<>
remove_desc(db_context ctx, const sstring& ks_name, const sstring& table_name) {
    try {
        return ctx._migration_manager.announce_column_family_drop(
                ks_name, desc_name(table_name), false);
    } catch (exceptions::configuration_exception& e) {
        // It's fine if the table does not exist.
        return make_ready_future<>();
    }
}

future<>
remove(db_context ctx, const sstring& ks_name, const sstring& table_name) {
    return when_all(remove_log(ctx, ks_name, table_name),
                    remove_desc(ctx, ks_name, table_name)).discard_result();
}

static future<> setup_log(db_context ctx, const schema& s) {
    schema_builder b(s.ks_name(), log_name(s.cf_name()));
    b.set_default_time_to_live(gc_clock::duration{86400}); // 24h
    b.set_comment(sprint("CDC log for %s.%s", s.ks_name(), s.cf_name()));
    b.with_column("stream_id", uuid_type, column_kind::partition_key);
    b.with_column("time", timeuuid_type, column_kind::clustering_key);
    b.with_column("batch_seq_no", int32_type, column_kind::clustering_key);
    b.with_column("operation", int32_type);
    b.with_column("ttl", long_type);
    auto add_columns = [&] (const auto& columns) {
        for (const auto& column : columns) {
            b.with_column("_" + column.name(), column.type);
        }
    };
    add_columns(s.partition_key_columns());
    add_columns(s.clustering_key_columns());
    add_columns(s.static_columns());
    add_columns(s.regular_columns());
    return ctx._migration_manager.announce_new_column_family(b.build(), false);
}

static future<> setup_desc(db_context ctx, const schema& s) {
    schema_builder b(s.ks_name(), desc_name(s.cf_name()));
    b.set_comment(sprint("CDC description for %s.%s", s.ks_name(), s.cf_name()));
    b.with_column("node_ip", inet_addr_type, column_kind::partition_key);
    b.with_column("shard_id", int32_type, column_kind::partition_key);
    b.with_column("created_at", timestamp_type, column_kind::clustering_key);
    b.with_column("stream_id", uuid_type);
    return ctx._migration_manager.announce_new_column_family(b.build(), false);
}

template <typename Func>
static void for_each_endpoint(db_context ctx, Func&& f) {
    for (const auto& dc : ctx._token_metadata.get_topology().get_datacenter_endpoints()) {
        for (const auto& endpoint : dc.second) {
            f(endpoint);
        }
    }
}

template <typename Func>
static void
for_each_shard(db_context ctx, const gms::inet_address& endpoint, Func&& f) {
    const unsigned int shard_count = ctx._snitch->get_shard_count(endpoint);
    for (unsigned int shard_id = 0; shard_id < shard_count; ++shard_id) {
        f(shard_id, shard_count);
    }
}

// This function assumes setup_desc was called on |s| before the call to this
// function.
static future<> populate_desc(db_context ctx, const schema& s) {
    auto& db = ctx._proxy.get_db().local();
    auto desc_schema =
        db.find_schema(s.ks_name(), desc_name(s.cf_name()));
    auto log_schema =
        db.find_schema(s.ks_name(), log_name(s.cf_name()));
    // The assumption here is that all nodes in the cluster are using the
    // same murmur3_partitioner_ignore_msb_bits. If it's not true, we will
    // have to gossip it around together with shard_count.
    auto ignore_msb_bits =
        db.get_config().murmur3_partitioner_ignore_msb_bits();

    auto belongs_to = [&](const gms::inet_address& endpoint,
                          const unsigned int shard_id,
                          const int shard_count,
                          const utils::UUID& stream_id) {
        const auto log_pk = partition_key::from_singular(*log_schema,
                                                         data_value(stream_id));
        const auto token = ctx._partitioner.decorate_key(*log_schema, log_pk).token();
        if (ctx._token_metadata.get_endpoint(ctx._token_metadata.first_token(token)) != endpoint) {
            return false;
        }
        const auto owning_shard_id = dht::murmur3_partitioner::shard_of(
                token, shard_count, ignore_msb_bits);
        return owning_shard_id == shard_id;
    };

    std::vector<mutation> mutations;
    const auto ts = api::new_timestamp();
    const auto ck = clustering_key::from_single_value(
            *desc_schema, timestamp_type->decompose(ts));
    auto cdef = desc_schema->get_column_definition(to_bytes("stream_id"));

    for_each_endpoint(ctx, [&] (const gms::inet_address& endpoint) {
        const auto decomposed_ip = inet_addr_type->decompose(endpoint.addr());

        for_each_shard(ctx, endpoint, [&] (unsigned int shard_id, int shard_count) {
            const auto pk = partition_key::from_exploded(
                    *desc_schema, { decomposed_ip, int32_type->decompose(static_cast<int>(shard_id)) });
            mutations.emplace_back(desc_schema, pk);

            auto stream_id = utils::make_random_uuid();
            while (!belongs_to(endpoint, shard_id, shard_count, stream_id)) {
                stream_id = utils::make_random_uuid();
            }
            auto value = atomic_cell::make_live(*uuid_type,
                                                ts,
                                                uuid_type->decompose(stream_id));
            mutations.back().set_cell(ck, *cdef, std::move(value));
        });
    });
    return ctx._proxy.mutate(std::move(mutations),
                             db::consistency_level::QUORUM,
                             db::no_timeout,
                             nullptr,
                             empty_service_permit());
}

future<> setup(db_context ctx, schema_ptr s) {
    return setup_log(ctx, *s).then([ctx, s = std::move(s)] () mutable {
        return make_ready_future<>().then([ctx, &s = *s] {
            return setup_desc(ctx, s);
        }).then_wrapped([ctx, s = std::move(s)] (future<> f) mutable {
            if (f.failed()) {
                return remove_log(ctx, s->ks_name(), s->cf_name());
            }
            return make_ready_future<>().then([ctx, &s = *s] {
                return populate_desc(ctx, s);
            }).handle_exception([ctx, s = std::move(s)] (std::exception_ptr ep) mutable {
                return remove_desc(ctx, s->ks_name(), s->cf_name()).then([ctx, s = std::move(s)] {
                    return remove_log(ctx, s->ks_name(), s->cf_name());
                }).then([ep = std::move(ep)] {
                    std::rethrow_exception(ep);
                });
            });
        });
    });
}

db_context db_context::builder::build() {
    return db_context{
        _proxy,
        _migration_manager ? _migration_manager->get() : service::get_local_migration_manager(),
        _token_metadata ? _token_metadata->get() : service::get_local_storage_service().get_token_metadata(),
        _snitch ? _snitch->get() : locator::i_endpoint_snitch::get_local_snitch_ptr(),
        _partitioner ? _partitioner->get() : dht::global_partitioner()
    };
}

class transformer final {
public:
    struct map_cmp {
        template <typename Func>
        bool cmp(const std::pair<net::inet_address, unsigned int>& a,
                 const std::pair<net::inet_address, unsigned int>& b,
                 Func&& func) const {
            auto&& a_ip = func(a.first).ip;
            auto&& b_ip = func(b.first).ip;
            return a_ip < b_ip || (a_ip == b_ip && a.second < b.second);
        }
        bool operator()(const std::pair<net::inet_address, unsigned int>& a,
                        const std::pair<net::inet_address, unsigned int>& b) const {
            if (a.first.is_ipv4()) {
                return !b.first.is_ipv4() ||
                    cmp(a, b, [] (const net::inet_address& addr) { return addr.as_ipv4_address(); });
            } else {
                return !b.first.is_ipv4() &&
                    cmp(a, b, [] (const net::inet_address& addr) { return addr.as_ipv6_address(); });
            }
        }
    };
    using streams_type = std::map<std::pair<net::inet_address, unsigned int>, utils::UUID, map_cmp>;
private:
    db_context _ctx;
    const schema& _schema;
    schema_ptr _log_schema;
    utils::UUID _time;
    bytes _decomposed_time;
    // The assumption here is that all nodes in the cluster are using the
    // same murmur3_partitioner_ignore_msb_bits. If it's not true, we will
    // have to gossip it around together with shard_count.
    unsigned _ignore_msb_bits;
    const streams_type& _streams;

    clustering_key set_pk_columns(const partition_key& pk, int batch_no, mutation& m) const {
        const auto log_ck = clustering_key::from_exploded(
                *m.schema(), { _decomposed_time, int32_type->decompose(batch_no) });
        auto pk_value = pk.explode(_schema);
        size_t pos = 0;
        for (const auto& column : _schema.partition_key_columns()) {
            assert (pos < pk_value.size());
            auto cdef = m.schema()->get_column_definition(to_bytes("_" + column.name()));
            auto value = atomic_cell::make_live(*column.type,
                                                _time.timestamp(),
                                                bytes_view(pk_value[pos]));
            m.set_cell(log_ck, *cdef, std::move(value));
            ++pos;
        }
        return log_ck;
    }
    partition_key stream_id(const net::inet_address& ip, unsigned int shard_id) const {
        auto it = _streams.find(std::make_pair(ip, shard_id));
        if (it == std::end(_streams)) {
                throw std::runtime_error(format("No stream found for node {} and shard {}", ip, shard_id));
        }
        return partition_key::from_exploded(*_log_schema, { uuid_type->decompose(it->second) });
    }
public:
    transformer(db_context ctx, const schema& s, const streams_type& streams)
        : _ctx(ctx)
        , _schema(s)
        , _log_schema(ctx._proxy.get_db().local().find_schema(_schema.ks_name(), log_name(_schema.cf_name())))
        , _time(utils::UUID_gen::get_time_UUID())
        , _decomposed_time(timeuuid_type->decompose(_time))
        , _ignore_msb_bits(ctx._proxy.get_db().local().get_config().murmur3_partitioner_ignore_msb_bits())
        , _streams(streams)
    { }

    mutation transform(const mutation& m) const {
        auto& t = m.token();
        auto&& ep = _ctx._token_metadata.get_endpoint(
                _ctx._token_metadata.first_token(t));
        if (!ep) {
            throw std::runtime_error(format("No owner found for key {}", m.decorated_key()));
        }
        auto shard_id = dht::murmur3_partitioner::shard_of(
                t, _ctx._snitch->get_shard_count(*ep), _ignore_msb_bits);
        mutation res(_log_schema, stream_id(ep->addr(), shard_id));
        set_pk_columns(m.key(), 0, res);
        return res;
    }
};

class streams_builder {
    const schema& _schema;
    transformer::streams_type _streams{transformer::map_cmp{}};
    net::inet_address _node_ip = net::inet_address();
    unsigned int _shard_id = 0;
    api::timestamp_type _latest_row_timestamp = api::min_timestamp;
    utils::UUID _latest_row_stream_id = utils::UUID();
public:
    streams_builder(const schema& s) : _schema(s) {}

    void accept_new_partition(const partition_key& key, uint32_t row_count) {
        auto exploded = key.explode(_schema);
        _node_ip = value_cast<net::inet_address>(inet_addr_type->deserialize(exploded[0]));
        _shard_id = static_cast<unsigned int>(value_cast<int>(int32_type->deserialize(exploded[1])));
        _latest_row_timestamp = api::min_timestamp;
        _latest_row_stream_id = utils::UUID();
    }

    void accept_new_partition(uint32_t row_count) {
        assert(false);
    }

    void accept_new_row(
            const clustering_key& key,
            const query::result_row_view& static_row,
            const query::result_row_view& row) {
        auto row_iterator = row.iterator();
        api::timestamp_type timestamp = value_cast<db_clock::time_point>(
                timestamp_type->deserialize(key.explode(_schema)[0])).time_since_epoch().count();
        if (timestamp <= _latest_row_timestamp) {
            return;
        }
        _latest_row_timestamp = timestamp;
        for (auto&& cdef : _schema.regular_columns()) {
            if (cdef.name_as_text() != "stream_id") {
                row_iterator.skip(cdef);
                continue;
            }
            auto val_opt = row_iterator.next_atomic_cell();
            assert(val_opt);
            val_opt->value().with_linearized([&] (bytes_view bv) {
                _latest_row_stream_id = value_cast<utils::UUID>(uuid_type->deserialize(bv));
            });
        }
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
        assert(false);
    }

    void accept_partition_end(const query::result_row_view& static_row) {
        _streams.emplace(std::make_pair(_node_ip, _shard_id), _latest_row_stream_id);
    }

    transformer::streams_type build() {
        return std::move(_streams);
    }
};

static future<transformer::streams_type> get_streams(
        db_context ctx,
        const sstring& ks_name,
        const sstring& cf_name,
        service::storage_proxy::clock_type::time_point timeout,
        service::query_state& qs) {
    auto s =
        ctx._proxy.get_db().local().find_schema(ks_name, desc_name(cf_name));
    query::read_command cmd(
            s->id(),
            s->version(),
            partition_slice_builder(*s).with_no_static_columns().build());
    return ctx._proxy.query(
            s,
            make_lw_shared(std::move(cmd)),
            {dht::partition_range::make_open_ended_both_sides()},
            db::consistency_level::QUORUM,
            {timeout, qs.get_permit(), qs.get_client_state()}).then([s = std::move(s)] (auto qr) mutable {
        return query::result_view::do_with(*qr.query_result,
                [s = std::move(s)] (query::result_view v) {
            auto slice = partition_slice_builder(*s)
                    .with_no_static_columns()
                    .build();
            streams_builder builder{ *s };
            v.consume(slice, builder);
            return builder.build();
        });
    });
}

future<std::vector<mutation>> append_log_mutations(
        db_context ctx,
        schema_ptr s,
        service::storage_proxy::clock_type::time_point timeout,
        service::query_state& qs,
        std::vector<mutation> muts) {
    return get_streams(ctx, s->ks_name(), s->cf_name(), timeout, qs).then(
            [ctx, s = std::move(s), muts = std::move(muts)]
            (transformer::streams_type streams) mutable {
        transformer trans(ctx, *s, streams);
        muts.reserve(2 * muts.size());
        for(int i = 0, size = muts.size(); i < size; ++i) {
            muts.push_back(trans.transform(muts[i]));
        }
        return std::move(muts);
    });
}

} // namespace cdc
