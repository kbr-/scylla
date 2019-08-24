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

#include "utils/chunked_vector.hh"
#include "schema_fwd.hh"
#include "gc_clock.hh"
#include "atomic_cell.hh"

class collection_type_impl;
class compaction_garbage_collector;
class row_tombstone;

// An auxiliary struct used to (de)construct collection_mutations.
// Unlike collection_mutation which is a serialized blob, this struct allows to inspect logical units of information
// (tombstone and cells) inside the mutation easily.
struct collection_mutation_description {
    tombstone tomb;
    utils::chunked_vector<std::pair<bytes, atomic_cell>> cells;

    // Expires cells based on query_time. Expires tombstones based on max_purgeable and gc_before.
    // Removes cells covered by tomb or this->tomb.
    bool compact_and_expire(column_id id, row_tombstone tomb, gc_clock::time_point query_time,
        can_gc_fn&, gc_clock::time_point gc_before, compaction_garbage_collector* collector = nullptr);
};

// Similar to collection_mutation_description, except that it doesn't own the information.
struct collection_mutation_view_description {
    tombstone tomb;
    utils::chunked_vector<std::pair<bytes_view, atomic_cell_view>> cells;

    collection_mutation_description materialize(const collection_type_impl&) const;
};

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
