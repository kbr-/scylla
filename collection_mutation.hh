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

// TODO rename/refactor, describe
class collection_mutation_view {
public:
    atomic_cell_value_view data;
};

// TODO fix this comment
// Represents a mutation of a collection (serialized).  Actual format is determined by collection type,
// and is:
//   set:  list of atomic_cell
//   map:  list of pair<atomic_cell, bytes> (for key/value)
//   list: tbd, probably ugly
class collection_mutation {
public:
    using imr_object_type =  imr::utils::object<data::cell::structure>;
    imr_object_type _data;

    collection_mutation() {}
    collection_mutation(const abstract_type&, collection_mutation_view v);
    collection_mutation(const abstract_type&, bytes_view bv);
    operator collection_mutation_view() const;
};
