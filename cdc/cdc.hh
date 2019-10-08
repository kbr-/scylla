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

#pragma once

#include <functional>
#include <optional>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

#include "service/storage_proxy.hh"
#include "timestamp.hh"

class schema;
using schema_ptr = seastar::lw_shared_ptr<const schema>;

namespace locator {

class snitch_ptr;
class token_metadata;

} // namespace locator

namespace service {

class migration_manager;
class query_state;

} // namespace service

namespace dht {

class i_partitioner;

} // namespace dht

class mutation;
class partition_key;

namespace cdc {

class streams_metadata;

struct db_context final {
    service::storage_proxy& _proxy;
    service::migration_manager& _migration_manager;
    locator::token_metadata& _token_metadata;
    const streams_metadata& _streams_metadata;
    locator::snitch_ptr& _snitch;
    dht::i_partitioner& _partitioner;

    class builder final {
        service::storage_proxy& _proxy;
        std::optional<std::reference_wrapper<service::migration_manager>> _migration_manager;
        std::optional<std::reference_wrapper<locator::token_metadata>> _token_metadata;
        std::optional<std::reference_wrapper<const streams_metadata>> _streams_metadata;
        std::optional<std::reference_wrapper<locator::snitch_ptr>> _snitch;
        std::optional<std::reference_wrapper<dht::i_partitioner>> _partitioner;
    public:
        builder(service::storage_proxy& proxy) : _proxy(proxy) { }

        builder with_migration_manager(service::migration_manager& migration_manager) {
            _migration_manager = migration_manager;
            return *this;
        }

        builder with_token_metadata(locator::token_metadata& token_metadata) {
            _token_metadata = token_metadata;
            return *this;
        }

        builder with_streams_metadata(streams_metadata& streams_metadata) {
            _streams_metadata = streams_metadata;
            return *this;
        }

        builder with_snitch(locator::snitch_ptr& snitch) {
            _snitch = snitch;
            return *this;
        }

        builder with_partitioner(dht::i_partitioner& partitioner) {
            _partitioner = partitioner;
            return *this;
        }

        db_context build();
    };
};

/// \brief Sets up CDC related tables for a given table
///
/// This function creates the CDC log table for a given base table.
///
/// param[in] ctx object with references to database components
/// param[in] schema schema of a table for which CDC tables are being created
seastar::future<> setup(db_context ctx, schema_ptr schema);

/// \brief Deletes CDC Log table for a given table
///
/// This function cleans up all CDC related tables created for a given table.
/// It's ok if some/all of them don't exist.
/// At the moment, the CDC Log is the only affected table.
///
/// \param[in] ctx object with references to database components
/// \param[in] ks_name keyspace name of a table for which CDC tables are removed
/// \param[in] table_name name of a table for which CDC tables are removed
///
/// \pre This function works correctly no matter if CDC Log exists.
seastar::future<>
remove(db_context ctx, const seastar::sstring& ks_name, const seastar::sstring& table_name);

seastar::sstring log_name(const seastar::sstring& table_name);

/// \brief For each mutation in the set appends related CDC Log mutation
///
/// This function should be called with a set of mutations of a table
/// with CDC enabled. Returned set of mutations contains all original mutations
/// and for each original mutation appends a mutation to CDC Log that reflects
/// the change.
///
/// \param[in] ctx object with references to database components
/// \param[in] s schema of a CDC enabled table which is being modified
/// \param[in] timeout period of time after which a request is considered timed out
/// \param[in] qs the state of the query that's being executed
/// \param[in] mutations set of changes of a CDC enabled table
///
/// \return set of mutations from input parameter with relevant CDC Log mutations appended
///
/// \pre CDC Log has to exist
seastar::future<std::vector<mutation>>append_log_mutations(
        db_context ctx,
        schema_ptr s,
        service::storage_proxy::clock_type::time_point timeout,
        service::query_state& qs,
        std::vector<mutation> mutations);

} // namespace cdc
