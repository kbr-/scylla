/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015 ScyllaDB
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

#include "cql3/user_types.hh"

#include "cql3/cql3_type.hh"
#include "cql3/constants.hh"

#include <boost/range/adaptor/transformed.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/algorithm/count.hpp>

#include "types/user.hh"

namespace cql3 {

shared_ptr<column_specification> user_types::field_spec_of(shared_ptr<column_specification> column, size_t field) {
    auto&& ut = static_pointer_cast<const user_type_impl>(column->type);
    auto&& name = ut->field_name(field);
    auto&& sname = sstring(reinterpret_cast<const char*>(name.data()), name.size());
    return make_shared<column_specification>(
                                   column->ks_name,
                                   column->cf_name,
                                   make_shared<column_identifier>(column->name->to_string() + "." + sname, true),
                                   ut->field_type(field));
}

user_types::literal::literal(elements_map_type entries)
        : _entries(std::move(entries)) {
}

shared_ptr<term> user_types::literal::prepare(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver) {
    validate_assignable_to(db, keyspace, receiver);
    auto&& ut = static_pointer_cast<const user_type_impl>(receiver->type);
    bool all_terminal = true;
    std::vector<shared_ptr<term>> values;
    values.reserve(_entries.size());
    size_t found_values = 0;
    for (size_t i = 0; i < ut->size(); ++i) {
        auto&& field = column_identifier(to_bytes(ut->field_name(i)), utf8_type);
        auto iraw = _entries.find(field);
        shared_ptr<term::raw> raw;
        if (iraw == _entries.end()) {
            raw = cql3::constants::NULL_LITERAL;
        } else {
            raw = iraw->second;
            ++found_values;
        }
        auto&& value = raw->prepare(db, keyspace, field_spec_of(receiver, i));

        if (dynamic_cast<non_terminal*>(value.get())) {
            all_terminal = false;
        }

        values.push_back(std::move(value));
    }
    if (found_values != _entries.size()) {
        // We had some field that are not part of the type
        for (auto&& id_val : _entries) {
            auto&& id = id_val.first;
            if (!boost::range::count(ut->field_names(), id.bytes_)) {
                throw exceptions::invalid_request_exception(format("Unknown field '{}' in value of user defined type {}", id, ut->get_name_as_string()));
            }
        }
    }

    delayed_value value(ut, values);
    if (all_terminal) {
        return value.bind(query_options::DEFAULT);
    } else {
        return make_shared(std::move(value));
    }
}

void user_types::literal::validate_assignable_to(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver) {
    // TODO kbr: is_user_type + static_cast
    auto&& ut = dynamic_pointer_cast<const user_type_impl>(receiver->type);
    if (!ut) {
        throw exceptions::invalid_request_exception(format("Invalid user type literal for {} of type {}", receiver->name, receiver->type->as_cql3_type()));
    }

    for (size_t i = 0; i < ut->size(); i++) {
        column_identifier field(to_bytes(ut->field_name(i)), utf8_type);
        if (_entries.count(field) == 0) {
            continue;
        }
        shared_ptr<term::raw> value = _entries[field];
        auto&& field_spec = field_spec_of(receiver, i);
        if (!assignment_testable::is_assignable(value->test_assignment(db, keyspace, field_spec))) {
            throw exceptions::invalid_request_exception(format("Invalid user type literal for {}: field {} is not of type {}", receiver->name, field, field_spec->type->as_cql3_type()));
        }
    }
}

assignment_testable::test_result user_types::literal::test_assignment(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver) {
    try {
        validate_assignable_to(db, keyspace, receiver);
        return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
    } catch (exceptions::invalid_request_exception& e) {
        return assignment_testable::test_result::NOT_ASSIGNABLE;
    }
}

sstring user_types::literal::assignment_testable_source_context() const {
    return to_string();
}

sstring user_types::literal::to_string() const {
    auto kv_to_str = [] (auto&& kv) { return format("{}:{}", kv.first, kv.second); };
    return format("{{{}}}", ::join(", ", _entries | boost::adaptors::transformed(kv_to_str)));
}

user_types::value::value(user_type type, std::vector<bytes_opt> elements)
        : _type(std::move(type)), _elements(std::move(elements)) {
    // TODO kbr: can one or the other be longer?
    // according do C*, types is always at least as long as elements
    std::cout << "TYPE SIZE: " << _type->size() << ", ELEMENTS SIZE: " << _elements.size() << std::endl;
    assert(_type->size() == _elements.size());
}

// TODO kbr: :(
user_types::value::value(user_type type, std::vector<bytes_view_opt> elements)
    : _type(std::move(type)), _elements(boost::copy_range<std::vector<bytes_opt>>(elements | boost::adaptors::transformed(
                [] (const bytes_view_opt& e) { return e ? bytes_opt(bytes(e->begin(), e->size())) : std::nullopt; }))) {}


shared_ptr<user_types::value> user_types::value::from_serialized(const fragmented_temporary_buffer::view& v, user_type type) {
    return with_linearized(v, [&] (bytes_view val) {
        auto elements = type->split(val);
        if (elements.size() > type->size()) {
            throw exceptions::invalid_request_exception(
                    format("User Defined Type value contained too many fields (expected {}, got {})", type->size(), elements.size()));
        }

        return ::make_shared<value>(type, elements);
    });
}

cql3::raw_value user_types::value::get(const query_options&) {
    return cql3::raw_value::make_value(tuple_type_impl::build_value(_elements));
}

const std::vector<bytes_opt>& user_types::value::get_elements() {
    return _elements;
}

sstring user_types::value::to_string() const {
    return "TODOFIXME kbr";
}

user_types::delayed_value::delayed_value(user_type type, std::vector<shared_ptr<term>> values)
        : _type(std::move(type)), _values(std::move(values)) {
}
bool user_types::delayed_value::uses_function(const sstring& ks_name, const sstring& function_name) const {
    return boost::algorithm::any_of(_values,
                std::bind(&term::uses_function, std::placeholders::_1, std::cref(ks_name), std::cref(function_name)));
}
bool user_types::delayed_value::contains_bind_marker() const {
    return boost::algorithm::any_of(_values, std::mem_fn(&term::contains_bind_marker));
}

void user_types::delayed_value::collect_marker_specification(shared_ptr<variable_specifications> bound_names) {
    for (auto&& v : _values) {
        v->collect_marker_specification(bound_names);
    }
}

std::vector<bytes_opt> user_types::delayed_value::bind_internal(const query_options& options) {
    auto sf = options.get_cql_serialization_format();
    std::vector<bytes_opt> buffers;
    // TODO kbr: check too big values size
    for (size_t i = 0; i < _type->size(); ++i) {
        const auto& value = _values[i]->bind_and_get(options);
        if (!_type->is_multi_cell() && value.is_unset_value()) {
            // TODO test this
            throw exceptions::invalid_request_exception(format("Invalid unset value for field '{}' of user defined type {}",
                        _type->field_name_as_string(i), _type->get_name_as_string()));
        }

        // TODO kbr: unset vs null
        buffers.push_back(to_bytes_opt(value));

        // Inside UDT values, we must force the serialization of collections to v3 whatever protocol
        // version is in use since we're going to store directly that serialized value.
        // TODO kbr: what about serialization of UDTs?
        if (!sf.collection_format_unchanged() && _type->field_type(i)->is_collection() && buffers.back()) {
            // TODO FIXME kbr
            auto&& ctype = static_pointer_cast<const collection_type_impl>(_type->field_type(i));
            // TODO kbr: latest? wtf
            buffers.back() = ctype->reserialize(sf, cql_serialization_format::latest(), bytes_view(*buffers.back()));
        }
    }
    return buffers;
}

shared_ptr<terminal> user_types::delayed_value::bind(const query_options& options) {
    return ::make_shared<user_types::value>(_type, bind_internal(options));
}

cql3::raw_value_view user_types::delayed_value::bind_and_get(const query_options& options) {
    return options.make_temporary(cql3::raw_value::make_value(user_type_impl::build_value(bind_internal(options))));
}

shared_ptr<terminal> user_types::marker::bind(const query_options& options) {
    auto value = options.get_value_at(_bind_index);
    if (value.is_null()) {
        return nullptr;
    }
    if (value.is_unset_value()) {
        return constants::UNSET_VALUE;
    }
    return value::from_serialized(*value, static_pointer_cast<const user_type_impl>(_receiver->type));
}

void user_types::setter::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    auto value = _t->bind(params._options);
    if (value == constants::UNSET_VALUE) {
        return;
    }

    // TODO FIXME kbr get rid of the cast
    auto typ = static_pointer_cast<const user_type_impl>(column.type);
    if (typ->is_multi_cell()) {
        std::cout << "MULTICELL!" << std::endl;
        // Non-frozen user defined type.

        collection_mutation_helper mut;

        // Setting a non-frozen (multi-cell) UDT means overwriting all cells.
        // We start by deleting all existing cells.
        mut.tomb = params.make_tombstone_just_before();

        if (value) {
            // TODO kbr: static cast
            auto ut_value = dynamic_pointer_cast<user_types::value>(value);
            assert(ut_value);

            const auto& elems = ut_value->get_elements();
            assert(typ->size() == elems.size());
            for (uint16_t i = 0; i < elems.size(); ++i) {
                if (!elems[i]) {
                    continue;
                } // TODO kbr: when does this happen?

                // The cell's 'key', in case of UDTs, is the index of the corresponding field.
                std::cout << "<<<I BUF" << std::endl;
                bytes i_buf(bytes::initialized_later(), sizeof(uint16_t));
                // TODO kbr: cast not needed?
                *reinterpret_cast<uint16_t*>(i_buf.begin()) = (uint16_t)net::hton(i);
                std::cout << ">>>I BUF" << std::endl;

                mut.cells.emplace_back(i_buf,
                        params.make_cell(*typ->type(i), *elems[i], atomic_cell::collection_member::yes));
            }
        }

        m.set_cell(row_key, column, serialize_collection_mutation(typ, std::move(mut)));
    } else {
        std::cout << "NOT MULTICELL!" << std::endl;
        if (value) {
            m.set_cell(row_key, column, make_cell(*typ, *value->get(params._options), params));
        } else {
            m.set_cell(row_key, column, make_dead_cell(params));
        }
    }
    std::cout << "SET CELL" << std::endl;
}

void user_types::setter_by_field::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    assert(column.type->is_user_type() && column.type->is_multi_cell());

    auto value = _t->bind(params._options);
    if (value == constants::UNSET_VALUE) {
        return;
    }

    auto type = static_pointer_cast<const user_type_impl>(column.type);

    auto idx_opt = type->idx_of_field(_field_name);
    assert(idx_opt);
    // TODO kbr: return uint16_t from idx_of_field?
    uint16_t idx = *idx_opt;

    // TODO kbr: copy paste
    bytes idx_buf(bytes::initialized_later(), sizeof(uint16_t));
    *reinterpret_cast<uint16_t*>(idx_buf.begin()) = (uint16_t)net::hton(idx);

    assert(idx < type->size());

    // TODO kbr can value->get(...) return unset_value? or is this equivalent to value == UNSET_VALUE?
    collection_mutation_helper mut;
    mut.cells.emplace_back(idx_buf, value
                ? params.make_cell(*type->type(idx), *value->get(params._options), atomic_cell::collection_member::yes)
                : make_dead_cell(params));

    m.set_cell(row_key, column, serialize_collection_mutation(type, std::move(mut)));
}

void user_types::deleter_by_field::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    assert(column.type->is_user_type() && column.type->is_multi_cell());

    // TODO kbr: copy paste
    bytes idx_buf(bytes::initialized_later(), sizeof(uint16_t));
    *reinterpret_cast<uint16_t*>(idx_buf.begin()) = (uint16_t)net::hton(_field_idx);

    collection_mutation_helper mut;
    mut.cells.emplace_back(idx_buf, make_dead_cell(params));

    // TODO kbr remove dispatch
    m.set_cell(row_key, column, serialize_collection_mutation(column.type, std::move(mut)));
}

}
