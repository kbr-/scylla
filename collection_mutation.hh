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

#include "atomic_cell.hh"

class collection_type_impl;

class collection_mutation_view {
public:
    atomic_cell_value_view data;
};

// A serialized mutation of a collection of cells.
// Used to represent mutations of collections (lists, maps, sets) or non-frozen user defined types.
// It contains a sequence of cells, each representing a mutation of a single entry (element or field) of the collection.
// Each cell has an associated 'key' (or 'path'). The meaning of each (key, cell) pair is:
//  for sets: the key is the serialized set element, the cell contains no data (except liveness information),
//  for maps: the key is the serialized map element's key, the cell contains the serialized map element's value,
//  for lists: the key is a timeuuid identifying the list entry, the cell contains the serialized value,
//  for user types: the key is an index identifying the field, the cell contains the value of the field.
//  The mutation may also contain a collection-wide tombstone.
class collection_mutation {
public:
    using imr_object_type =  imr::utils::object<data::cell::structure>;
    imr_object_type _data;

    collection_mutation() {}
    collection_mutation(const collection_type_impl&, collection_mutation_view);
    collection_mutation(const collection_type_impl&, bytes_view);
    operator collection_mutation_view() const;
};
