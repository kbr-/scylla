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
#include "cdc/streams_metadata.hh"

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

future<>
remove(db_context ctx, const sstring& ks_name, const sstring& table_name) {
    return remove_log(ctx, ks_name, table_name);
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

future<> setup(db_context ctx, schema_ptr s) {
    return setup_log(ctx, *s).then([s = std::move(s)] {});
}

db_context db_context::builder::build() {
    return db_context{
        _proxy,
        _migration_manager ? _migration_manager->get() : service::get_local_migration_manager(),
        _token_metadata ? _token_metadata->get() : service::get_local_storage_service().get_token_metadata(),
        _streams_metadata ? _streams_metadata->get() : service::get_local_storage_service().get_streams_metadata(),
        _snitch ? _snitch->get() : locator::i_endpoint_snitch::get_local_snitch_ptr(),
        _partitioner ? _partitioner->get() : dht::global_partitioner()
    };
}

class transformer final {
    db_context _ctx;
    const schema& _schema;
    schema_ptr _log_schema;
    utils::UUID _time;
    bytes _decomposed_time;
    unsigned _ignore_msb_bits;

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

public:
    transformer(db_context ctx, const schema& s)
        : _ctx(ctx)
        , _schema(s)
        , _log_schema(ctx._proxy.get_db().local().find_schema(_schema.ks_name(), log_name(_schema.cf_name())))
        , _time(utils::UUID_gen::get_time_UUID())
        , _decomposed_time(timeuuid_type->decompose(_time))
        , _ignore_msb_bits(ctx._proxy.get_db().local().get_config().murmur3_partitioner_ignore_msb_bits())
    { }

    mutation transform(const mutation& m) const {
        auto stream = _ctx._streams_metadata.stream_for_token(m.token(), _ctx._token_metadata, _ignore_msb_bits);
        mutation res(_log_schema, partition_key::from_exploded(*_log_schema, { uuid_type->decompose(stream) }));

        auto& p = m.partition();
        if(p.partition_tombstone()) {
            // Partition deletion
            set_pk_columns(m.key(), 0, res);
        } else if (!p.row_tombstones().empty()) {
            // range deletion
            int batch_no = 0;
            for (auto& rt : p.row_tombstones()) {
                auto set_bound = [&] (const clustering_key& log_ck, const clustering_key_prefix& ckp) {
                    auto exploded = ckp.explode(_schema);
                    size_t pos = 0;
                    for (const auto& column : _schema.clustering_key_columns()) {
                        if (pos >= exploded.size()) {
                            break;
                        }
                        auto cdef = _log_schema->get_column_definition(to_bytes("_" + column.name()));
                        auto value = atomic_cell::make_live(*column.type,
                                                            _time.timestamp(),
                                                            bytes_view(exploded[pos]));
                        res.set_cell(log_ck, *cdef, std::move(value));
                        ++pos;
                    }
                };
                {
                    auto log_ck = set_pk_columns(m.key(), batch_no, res);
                    set_bound(log_ck, rt.start);
                    ++batch_no;
                }
                {
                    auto log_ck = set_pk_columns(m.key(), batch_no, res);
                    set_bound(log_ck, rt.end);
                    ++batch_no;
                }
            }
        } else {
            // should be update or deletion
            int batch_no = 0;
            for (const rows_entry& r : p.clustered_rows()) {
                auto log_ck = set_pk_columns(m.key(), batch_no, res);
                auto ck_value = r.key().explode(_schema);
                size_t pos = 0;
                for (const auto& column : _schema.clustering_key_columns()) {
                    assert (pos < ck_value.size());
                    auto cdef = _log_schema->get_column_definition(to_bytes("_" + column.name()));
                    auto value = atomic_cell::make_live(*column.type,
                                                        _time.timestamp(),
                                                        bytes_view(ck_value[pos]));
                    res.set_cell(log_ck, *cdef, std::move(value));
                    ++pos;
                }

                ++batch_no;
            }
        }
        return res;
    }
};

future<std::vector<mutation>> append_log_mutations(
        db_context ctx,
        schema_ptr s,
        service::storage_proxy::clock_type::time_point timeout,
        service::query_state& qs,
        std::vector<mutation> muts) {
    transformer trans(ctx, *s);
    muts.reserve(2 * muts.size());
    for(int i = 0, size = muts.size(); i < size; ++i) {
        muts.push_back(trans.transform(muts[i]));
    }
    return make_ready_future<std::vector<mutation>>(muts);
}

} // namespace cdc
