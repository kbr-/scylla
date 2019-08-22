/*
 * Copyright (C) 2014 ScyllaDB
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

#include "types.hh"
#include "types/tuple.hh"

class user_type_impl : public tuple_type_impl {
    using intern = type_interning_helper<user_type_impl, sstring, bytes, std::vector<bytes>, std::vector<data_type>, bool>;
public:
    const sstring _keyspace;
    const bytes _name;
private:
    const std::vector<bytes> _field_names;
    const std::vector<sstring> _string_field_names;
    const bool _is_multi_cell;
public:
    using native_type = std::vector<data_value>;
    user_type_impl(sstring keyspace, bytes name, std::vector<bytes> field_names, std::vector<data_type> field_types, bool is_multi_cell)
            : tuple_type_impl(make_name(keyspace, name, field_names, field_types, is_multi_cell), field_types, false /* freezeInner */)
            , _keyspace(std::move(keyspace))
            , _name(std::move(name))
            , _field_names(std::move(field_names))
            , _string_field_names(boost::copy_range<std::vector<sstring>>(_field_names | boost::adaptors::transformed(
                    [] (const bytes& field_name) { return utf8_type->to_string(field_name); })))
            , _is_multi_cell(is_multi_cell) {
    }
    static shared_ptr<const user_type_impl> get_instance(sstring keyspace, bytes name, std::vector<bytes> field_names, std::vector<data_type> field_types, bool is_multi_cell) {
        return intern::get_instance(std::move(keyspace), std::move(name), std::move(field_names), std::move(field_types), is_multi_cell);
    }
    data_type field_type(size_t i) const { return type(i); }
    const std::vector<data_type>& field_types() const { return _types; }
    bytes_view field_name(size_t i) const { return _field_names[i]; }
    sstring field_name_as_string(size_t i) const { return _string_field_names[i]; }
    const std::vector<bytes>& field_names() const { return _field_names; }
    std::optional<uint32_t> idx_of_field(const bytes& name) const;
    virtual bool is_multi_cell() const override { return _is_multi_cell; }
    virtual data_type freeze() const override;
    sstring get_name_as_string() const;
    virtual sstring cql3_type_name_impl() const override;
    virtual bool is_native() const override { return false; }
    virtual bool equals(const abstract_type& other) const override;
    virtual bool is_value_compatible_with_internal(const abstract_type&) const override;
    virtual bool is_user_type() const override { return true; }
    virtual bool references_user_type(const sstring& keyspace, const bytes& name) const override;
    virtual std::optional<data_type> update_user_type(const shared_ptr<const user_type_impl> updated) const override;

    virtual sstring to_json_string(bytes_view bv) const override;
    virtual bytes from_json_object(const Json::Value& value, cql_serialization_format sf) const override;
private:
    static sstring make_name(sstring keyspace,
                             bytes name,
                             std::vector<bytes> field_names,
                             std::vector<data_type> field_types,
                             bool is_multi_cell);
};

data_value make_user_value(data_type tuple_type, user_type_impl::native_type value);

