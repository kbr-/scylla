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

#include <boost/type.hpp>

#include "schema_fwd.hh"
// TODO kbr: row_tombstone
// this one includes many other...
// #include "mutation_partition.hh"
// TODO kbr:
#include "gc_clock.hh"
// TODO kbr:
#include "utils/chunked_vector.hh"
// TODO kbr: serializer.hh requires boost::type
#include "serializer.hh"
#include "serializer_impl.hh"
#include "serialization_visitors.hh"
// TODO kbr: needed to include idl mutation... can this be fixed?
// #include "counters.hh"
// #include "range_tombstone.hh"
// #include "mutation_framgent.hh"
// #include "idl/mutation.dist.hh"
// #include "idl/mutation.dist.impl.hh"

// for data_type...
#include "types.hh"

class compaction_garbage_collector;
class row_tombstone;

// TODO rename...
// A helper struct used to (de)construct collection_mutations. (TODO HOW?)
// Unlike collection_mutation which is a serialized blob, this struct allows to inspect logical units of information
// (tombstone and/or cells) inside the mutation easily.
struct collection_mutation_helper {
    tombstone tomb;
    utils::chunked_vector<std::pair<bytes, atomic_cell>> cells;

    // Expires cells based on query_time. Expires tombstones based on max_purgeable and gc_before.
    // Removes cells covered by tomb or this->tomb.
    bool compact_and_expire(column_id id, row_tombstone tomb, gc_clock::time_point query_time,
        can_gc_fn&, gc_clock::time_point gc_before, compaction_garbage_collector* collector = nullptr);
};

// Similar to  collection_mutation_helper, except working with views.
struct collection_mutation_view_helper {
    tombstone tomb;
    utils::chunked_vector<std::pair<bytes_view, atomic_cell_view>> cells;

    collection_mutation_helper materialize(const data_type&) const;
};

// Given a linearized collection_mutation_view, returns a helper struct allowing the inspection of each cell.
// The helper is an observer of the data given by the collection_mutation_view and doesn't extend its lifetime.
// It requires a type (collection or user defined) to reconstruct the structural information.
collection_mutation_view_helper deserialize_collection_mutation(const data_type&, bytes_view);

// TODO rename/refactor, describe
class collection_mutation_view {
public:
    // TODO kbr: private?
    atomic_cell_value_view data;

    // Is this a noop mutation? (TODO kbr: really?)
    bool is_empty() const;

    // Is any of the stored cells live (not deleted nor expired) at the time point `tp`,
    // given the later of the tombstones `t` and the one stored in the mutation (if any)?
    // It requires a type to reconstruct the structural information.
    bool is_any_live(const data_type&, tombstone t = tombstone(), gc_clock::time_point tp = gc_clock::time_point::min()) const;

    // The maximum of timestamps of the mutation's cells and tombstone.
    api::timestamp_type last_update(const data_type&) const;

    // auto F(collection_mutation_helper)
    template <typename F>
    inline decltype(auto) with_deserialized(const data_type& typ, F&& f) const {
        return data.with_linearized([&typ, f = std::forward<F>(f)] (bytes_view bv) {
            // TODO kbr: two dispatches...
            auto m_view = deserialize_collection_mutation(typ, bv);
            auto m = m_view.materialize(typ);
            // TODO forward?
            return f(std::move(m));
        });
    }

    // auto F(collection_mutation_helper_view)
    template <typename F>
    inline decltype(auto) with_deserialized_view(const data_type& typ, F&& f) const {
        return data.with_linearized([&typ, f = std::forward<F>(f)] (bytes_view bv) {
            auto m_view = deserialize_collection_mutation(typ, bv);
            // TODO forward?
            return f(std::move(m_view));
        });
    }
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

// TODO doc
// FIXME: use iterators?
// TODO: by value version?
collection_mutation serialize_collection_mutation(const data_type&, const collection_mutation_helper&);
collection_mutation serialize_collection_mutation(const data_type&, const collection_mutation_view_helper&);

// TODO doc
collection_mutation merge(const data_type& typ, collection_mutation_view a, collection_mutation_view b);

// TODO doc
collection_mutation difference(const data_type& typ, collection_mutation_view a, collection_mutation_view b);

    // Calls Func(atomic_cell_view) for each cell in this collection.
    // noexcept if Func doesn't throw.
    // TODO kbr: ??
    // template<typename Func>
    // void for_each_cell(collection_mutation_view c, Func&& func) const {
    //   c.data.with_linearized([&] (bytes_view c_bv) {
    //     auto m_view = deserialize_mutation_form(c_bv);
    //     for (auto&& c : m_view.cells) {
    //         func(std::move(c.second));
    //     }
    //   });
    // }
