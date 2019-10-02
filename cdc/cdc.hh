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

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

class schema;
using schema_ptr = seastar::lw_shared_ptr<const schema>;

namespace locator {

class snitch_ptr;
class token_metadata;

} // namespace locator

namespace service {

class migration_manager;
class storage_proxy;

} // namespace service

namespace dht {

class i_partitioner;

} // namespace dht

namespace cdc {

struct db_context final {
    service::storage_proxy& _proxy;
    service::migration_manager& _migration_manager;
    locator::token_metadata& _token_metadata;
    locator::snitch_ptr& _snitch;
    dht::i_partitioner& _partitioner;

    class builder final {
        service::storage_proxy& _proxy;
        std::optional<std::reference_wrapper<service::migration_manager>> _migration_manager;
        std::optional<std::reference_wrapper<locator::token_metadata>> _token_metadata;
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
/// This function not only creates CDC Log and CDC Description for a given table
/// but also populates CDC Description with a list of change streams.
///
/// param[in] ctx object with references to database components
/// param[in] schema schema of a table for which CDC tables are being created
seastar::future<> setup(db_context ctx, schema_ptr schema);

/// \brief Deletes CDC Log and CDC Description tables for a given table
///
/// This function cleans up all CDC related tables created for a given table.
/// At the moment, CDC Log and CDC Description are the only affected tables.
/// It's ok if some/all of them don't exist.
///
/// \param[in] ctx object with references to database components
/// \param[in] ks_name keyspace name of a table for which CDC tables are removed
/// \param[in] table_name name of a table for which CDC tables are removed
///
/// \pre This function works correctly no matter CDC Log and/or CDC Describtion
///      exist.
seastar::future<>
remove(db_context ctx, const seastar::sstring& ks_name, const seastar::sstring& table_name);

seastar::sstring log_name(const seastar::sstring& table_name);

seastar::sstring desc_name(const seastar::sstring& table_name);

} // namespace cdc
