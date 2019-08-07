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

#pragma once

#include <vector>

#include <seastar/core/shared_ptr.hh>

#include "schema.hh"
#include "sstables/types.hh"
#include "utils/UUID.hh"
#include "db/marshal/type_parser.hh"

namespace sstables {

using column_values_fixed_lengths = std::vector<std::optional<uint32_t>>;

/*
 * A helper method to get fixed lengths of clustering key values
 */
inline column_values_fixed_lengths get_clustering_values_fixed_lengths(const serialization_header& header) {
    column_values_fixed_lengths lengths;
    lengths.reserve(header.clustering_key_types_names.elements.size());
    for (auto&& t : header.clustering_key_types_names.elements) {
        auto type = db::marshal::type_parser::parse(to_sstring_view(t.value));
        lengths.push_back(type->value_length_if_fixed());
    }

    return lengths;
}

/*
 * This class caches a mapping from columns present in sstable to their column_id.
 * This way we don't need to looku them up by column name every time.
 */
class column_translation {
public:
    struct column_info {
        const bytes* name = nullptr;
        data_type type;
        // Disengaged 'id' means the column is missing from the current schema
        std::optional<column_id> id;
        std::optional<uint32_t> value_length;
        bool is_collection;
        bool is_counter;
        bool schema_mismatch;
    };

private:

    struct state {

        static std::vector<column_info> build(
                const schema& s,
                const utils::chunked_vector<serialization_header::column_desc>& src,
                bool is_static) {
            std::vector<column_info> cols;
            if (s.is_dense()) {
                const column_definition& col = is_static ? *s.static_begin() : *s.regular_begin();
                cols.push_back(column_info{
                    &col.name(),
                    col.type,
                    col.id,
                    col.type->value_length_if_fixed(),
                    col.is_multi_cell(),
                    col.is_counter(),
                    false
                });
            } else {
                cols.reserve(src.size());
                for (auto&& desc : src) {
                    const bytes& type_name = desc.type_name.value;
                    data_type type = db::marshal::type_parser::parse(to_sstring_view(type_name));
                    const column_definition* def = s.get_column_definition(desc.name.value);
                    std::optional<column_id> id;
                    bool schema_mismatch = false;
                    if (def) {
                        id = def->id;
                        if (def->is_multi_cell() != type->is_multi_cell()) {
                            throw std::runtime_error(format("SCHEMA MISMATCH def {} type {}\n", def->is_multi_cell(), type->is_multi_cell()));
                        }
                        schema_mismatch = def->is_multi_cell() != type->is_multi_cell() ||
                                          def->is_counter() != type->is_counter() ||
                                          !def->type->is_value_compatible_with(*type);
                    }
                    cols.push_back(column_info{
                        &desc.name.value,
                        type,
                        id,
                        type->value_length_if_fixed(),
                        type->is_multi_cell(),
                        type->is_counter(),
                        schema_mismatch
                    });
                }
                boost::range::stable_partition(cols, [](const column_info& column) { return !column.is_collection; });
            }
            return cols;
        }

        utils::UUID schema_uuid;
        std::vector<column_info> regular_schema_columns_from_sstable;
        std::vector<column_info> static_schema_columns_from_sstable;
        column_values_fixed_lengths clustering_column_value_fix_lengths;

        state() = default;
        state(const state&) = delete;
        state& operator=(const state&) = delete;
        state(state&&) = default;
        state& operator=(state&&) = default;

        state(const schema& s, const serialization_header& header)
            : schema_uuid(s.version())
            , regular_schema_columns_from_sstable(build(s, header.regular_columns.elements, false))
            , static_schema_columns_from_sstable(build(s, header.static_columns.elements, true))
            , clustering_column_value_fix_lengths (get_clustering_values_fixed_lengths(header))
        {}
    };

    lw_shared_ptr<const state> _state = make_lw_shared<const state>();

public:
    column_translation get_for_schema(const schema& s, const serialization_header& header) {
        if (s.version() != _state->schema_uuid) {
            _state = make_lw_shared(state(s, header));
        }
        return *this;
    }

    const std::vector<column_info>& regular_columns() const {
        return _state->regular_schema_columns_from_sstable;
    }
    const std::vector<column_info>& static_columns() const {
        return _state->static_schema_columns_from_sstable;
    }
    const std::vector<std::optional<uint32_t>>& clustering_column_value_fix_legths() const {
        return _state->clustering_column_value_fix_lengths;
    }
};

};   // namespace sstables
