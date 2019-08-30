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

#include "types/collection.hh"
#include "types/user.hh"
#include "concrete_types.hh"
#include "atomic_cell_or_collection.hh"
#include "mutation_partition.hh"
#include "compaction_garbage_collector.hh"
#include "combine.hh"

#include "collection_mutation.hh"

collection_mutation::collection_mutation(const abstract_type& type, collection_mutation_view v)
    : _data(imr_object_type::make(data::cell::make_collection(v.data), &type.imr_state().lsa_migrator())) {}

collection_mutation::collection_mutation(const abstract_type& type, bytes_view v)
    : _data(imr_object_type::make(data::cell::make_collection(v), &type.imr_state().lsa_migrator())) {}

static collection_mutation_view get_collection_mutation_view(const uint8_t* ptr)
{
    auto f = data::cell::structure::get_member<data::cell::tags::flags>(ptr);
    auto ti = data::type_info::make_collection();
    data::cell::context ctx(f, ti);
    auto view = data::cell::structure::get_member<data::cell::tags::cell>(ptr).as<data::cell::tags::collection>(ctx);
    auto dv = data::cell::variable_value::make_view(view, f.get<data::cell::tags::external_data>());
    return collection_mutation_view { dv };
}

collection_mutation::operator collection_mutation_view() const
{
    return get_collection_mutation_view(_data.get());
}

collection_mutation_view atomic_cell_or_collection::as_collection_mutation() const {
    return get_collection_mutation_view(_data.get());
}

bool collection_mutation_view::is_empty() const {
  return data.with_linearized([&] (bytes_view in) { // FIXME: we can guarantee that this is in the first fragment
    auto has_tomb = read_simple<bool>(in);
    return !has_tomb && read_simple<uint32_t>(in) == 0;
  });
}

bool collection_mutation_view::is_any_live(const abstract_type& type, tombstone tomb, gc_clock::time_point now) const {
  return data.with_linearized([&] (bytes_view in) {
    auto has_tomb = read_simple<bool>(in);
    if (has_tomb) {
        auto ts = read_simple<api::timestamp_type>(in);
        auto ttl = read_simple<gc_clock::duration::rep>(in);
        tomb.apply(tombstone{ts, gc_clock::time_point(gc_clock::duration(ttl))});
    }

    auto nr = read_simple<uint32_t>(in);

    return visit(type, make_visitor(
        [&] (const collection_type_impl& ctype) {
            auto& type_info = ctype.value_comparator()->imr_state().type_info();
            for (uint32_t i = 0; i != nr; ++i) {
                auto ksize = read_simple<uint32_t>(in);
                in.remove_prefix(ksize);
                auto vsize = read_simple<uint32_t>(in);
                auto value = atomic_cell_view::from_bytes(type_info, read_simple_bytes(in, vsize));
                if (value.is_live(tomb, now, false)) {
                    return true;
                }
            }
            return false;
        },
        [&] (const user_type_impl& utype) {
            for (uint32_t i = 0; i != nr; ++i) {
                auto ksize = read_simple<uint32_t>(in);
                auto key = read_simple_bytes(in, ksize);
                auto vsize = read_simple<uint32_t>(in);
                auto value = atomic_cell_view::from_bytes(
                        utype.type(deserialize_field_index(key))->imr_state().type_info(), read_simple_bytes(in, vsize));
                if (value.is_live(tomb, now, false)) {
                    return true;
                }
            }
            return false;
        },
        [&] (const abstract_type& o) -> bool {
            throw std::runtime_error(format("collection_mutation_view::is_any_live: unknown type {}", o.name()));
        }
    ));
  });
}

api::timestamp_type collection_mutation_view::last_update(const abstract_type& type) const {
    assert(type.is_collection());
    auto& ctype = static_cast<const collection_type_impl&>(type);

  return data.with_linearized([&] (bytes_view in) {
    api::timestamp_type max = api::missing_timestamp;
    auto has_tomb = read_simple<bool>(in);
    if (has_tomb) {
        max = std::max(max, read_simple<api::timestamp_type>(in));
        (void)read_simple<gc_clock::duration::rep>(in);
    }
    auto nr = read_simple<uint32_t>(in);
    for (uint32_t i = 0; i != nr; ++i) {
        auto ksize = read_simple<uint32_t>(in);
        in.remove_prefix(ksize);
        auto vsize = read_simple<uint32_t>(in);
        auto value = atomic_cell_view::from_bytes(ctype.value_comparator()->imr_state().type_info(), read_simple_bytes(in, vsize));
        max = std::max(value.timestamp(), max);
    }
    return max;
  });
}

collection_mutation_description
collection_mutation_view_description::materialize(const abstract_type& type) const {
    collection_mutation_description m;
    m.tomb = tomb;
    m.cells.reserve(cells.size());

    visit(type, make_visitor(
        [&] (const collection_type_impl& ctype) {
            auto& value_type = *ctype.value_comparator();
            for (auto&& e : cells) {
                m.cells.emplace_back(to_bytes(e.first), atomic_cell(value_type, e.second));
            }
        },
        [&] (const user_type_impl& utype) {
            for (auto&& e : cells) {
                m.cells.emplace_back(to_bytes(e.first), atomic_cell(*utype.type(deserialize_field_index(e.first)), e.second));
            }
        },
        [&] (const abstract_type& o) {
            throw std::runtime_error(format("attempted to materialize collection_mutation_view_description with type {}", o.name()));
        }
    ));

    return m;
}

bool collection_mutation_description::compact_and_expire(column_id id, row_tombstone base_tomb, gc_clock::time_point query_time,
    can_gc_fn& can_gc, gc_clock::time_point gc_before, compaction_garbage_collector* collector)
{
    bool any_live = false;
    auto t = tomb;
    tombstone purged_tomb;
    if (tomb <= base_tomb.regular()) {
        tomb = tombstone();
    } else if (tomb.deletion_time < gc_before && can_gc(tomb)) {
        purged_tomb = tomb;
        tomb = tombstone();
    }
    t.apply(base_tomb.regular());
    utils::chunked_vector<std::pair<bytes, atomic_cell>> survivors;
    utils::chunked_vector<std::pair<bytes, atomic_cell>> losers;
    for (auto&& name_and_cell : cells) {
        atomic_cell& cell = name_and_cell.second;
        auto cannot_erase_cell = [&] {
            return cell.deletion_time() >= gc_before || !can_gc(tombstone(cell.timestamp(), cell.deletion_time()));
        };

        if (cell.is_covered_by(t, false) || cell.is_covered_by(base_tomb.shadowable().tomb(), false)) {
            continue;
        }
        if (cell.has_expired(query_time)) {
            if (cannot_erase_cell()) {
                survivors.emplace_back(std::make_pair(
                    std::move(name_and_cell.first), atomic_cell::make_dead(cell.timestamp(), cell.deletion_time())));
            } else if (collector) {
                losers.emplace_back(std::pair(
                        std::move(name_and_cell.first), atomic_cell::make_dead(cell.timestamp(), cell.deletion_time())));
            }
        } else if (!cell.is_live()) {
            if (cannot_erase_cell()) {
                survivors.emplace_back(std::move(name_and_cell));
            } else if (collector) {
                losers.emplace_back(std::move(name_and_cell));
            }
        } else {
            any_live |= true;
            survivors.emplace_back(std::move(name_and_cell));
        }
    }
    if (collector) {
        collector->collect(id, collection_mutation_description{purged_tomb, std::move(losers)});
    }
    cells = std::move(survivors);
    return any_live;
}

template <typename Iterator>
static collection_mutation serialize_collection_mutation(
        const abstract_type& type,
        const tombstone& tomb,
        boost::iterator_range<Iterator> cells) {
    auto element_size = [] (size_t c, auto&& e) -> size_t {
        return c + 8 + e.first.size() + e.second.serialize().size();
    };
    auto size = accumulate(cells, (size_t)4, element_size);
    size += 1;
    if (tomb) {
        size += sizeof(tomb.timestamp) + sizeof(tomb.deletion_time);
    }
    bytes ret(bytes::initialized_later(), size);
    bytes::iterator out = ret.begin();
    *out++ = bool(tomb);
    if (tomb) {
        write(out, tomb.timestamp);
        write(out, tomb.deletion_time.time_since_epoch().count());
    }
    auto writeb = [&out] (bytes_view v) {
        serialize_int32(out, v.size());
        out = std::copy_n(v.begin(), v.size(), out);
    };
    // FIXME: overflow?
    serialize_int32(out, boost::distance(cells));
    for (auto&& kv : cells) {
        auto&& k = kv.first;
        auto&& v = kv.second;
        writeb(k);

        writeb(v.serialize());
    }
    return collection_mutation(type, ret);
}

collection_mutation collection_mutation_description::serialize(const abstract_type& type) const {
    return serialize_collection_mutation(type, tomb, boost::make_iterator_range(cells.begin(), cells.end()));
}

collection_mutation collection_mutation_view_description::serialize(const abstract_type& type) const {
    return serialize_collection_mutation(type, tomb, boost::make_iterator_range(cells.begin(), cells.end()));
}

template <typename F>
static collection_mutation_view_description merge(collection_mutation_view_description a, collection_mutation_view_description b, F key_compare) {
    using element_type = std::pair<bytes_view, atomic_cell_view>;

    auto compare = [key_compare = std::move(key_compare)] (const element_type& e1, const element_type& e2) {
        return key_compare(e1.first, e2.first);
    };

    auto merge = [] (const element_type& e1, const element_type& e2) {
        // FIXME: use std::max()?
        return std::make_pair(e1.first, compare_atomic_cell_for_merge(e1.second, e2.second) > 0 ? e1.second : e2.second);
    };

    // applied to a tombstone, returns a predicate checking whether a cell is killed by
    // the tombstone
    auto cell_killed = [] (const std::optional<tombstone>& t) {
        return [&t] (const element_type& e) {
            if (!t) {
                return false;
            }
            // tombstone wins if timestamps equal here, unlike row tombstones
            if (t->timestamp < e.second.timestamp()) {
                return false;
            }
            return true;
            // FIXME: should we consider TTLs too?
        };
    };

    collection_mutation_view_description merged;
    merged.cells.reserve(a.cells.size() + b.cells.size());

    combine(a.cells.begin(), std::remove_if(a.cells.begin(), a.cells.end(), cell_killed(b.tomb)),
            b.cells.begin(), std::remove_if(b.cells.begin(), b.cells.end(), cell_killed(a.tomb)),
            std::back_inserter(merged.cells),
            compare,
            merge);
    merged.tomb = std::max(a.tomb, b.tomb);

    return merged;
}

collection_mutation merge(const abstract_type& type, collection_mutation_view a, collection_mutation_view b) {
    return a.with_deserialized(type, [&] (collection_mutation_view_description a_view) {
    return b.with_deserialized(type, [&] (collection_mutation_view_description b_view) {

    return visit(type, make_visitor(
        [&] (const collection_type_impl& ctype) {
            return merge(std::move(a_view), std::move(b_view), [&key_type = *ctype.name_comparator()] (bytes_view k1, bytes_view k2) {
                return key_type.less(k1, k2);
            });
        },
        [&] (const user_type_impl& utype) {
            return merge(std::move(a_view), std::move(b_view), [] (bytes_view k1, bytes_view k2) {
                return deserialize_field_index(k1) < deserialize_field_index(k2);
            });
        },
        [] (const abstract_type& o) -> collection_mutation_view_description {
            throw std::runtime_error(format("collection_mutation merge: unknown type: {}", o.name()));
        }
    )).serialize(type);

    });});
}

collection_mutation difference(const abstract_type& type, collection_mutation_view a, collection_mutation_view b)
{
    return a.with_deserialized(type, [&] (collection_mutation_view_description a_view) {
    return b.with_deserialized(type, [&] (collection_mutation_view_description b_view) {

    assert(type.is_collection());
    auto& ctype = static_cast<const collection_type_impl&>(type);

    collection_mutation_view_description diff;
    diff.cells.reserve(std::max(a_view.cells.size(), b_view.cells.size()));

    auto key_type = ctype.name_comparator();

    auto it = b_view.cells.begin();
    for (auto&& c : a_view.cells) {
        while (it != b_view.cells.end() && key_type->less(it->first, c.first)) {
            ++it;
        }
        if (it == b_view.cells.end() || !key_type->equal(it->first, c.first)
            || compare_atomic_cell_for_merge(c.second, it->second) > 0) {

            auto cell = std::make_pair(c.first, c.second);
            diff.cells.emplace_back(std::move(cell));
        }
    }
    if (a_view.tomb > b_view.tomb) {
        diff.tomb = a_view.tomb;
    }
    return diff.serialize(type);

    });});
}

collection_mutation_view_description
deserialize_collection_mutation(const abstract_type& type, bytes_view in) {
    collection_mutation_view_description ret;
    auto has_tomb = read_simple<bool>(in);
    if (has_tomb) {
        auto ts = read_simple<api::timestamp_type>(in);
        auto ttl = read_simple<gc_clock::duration::rep>(in);
        ret.tomb = tombstone{ts, gc_clock::time_point(gc_clock::duration(ttl))};
    }
    auto nr = read_simple<uint32_t>(in);
    ret.cells.reserve(nr);

    visit(type, make_visitor(
        [&] (const collection_type_impl& ctype) {
            // value_comparator(), ugh
            auto& type_info = ctype.value_comparator()->imr_state().type_info();
            for (uint32_t i = 0; i != nr; ++i) {
                // FIXME: we could probably avoid the need for size
                auto ksize = read_simple<uint32_t>(in);
                auto key = read_simple_bytes(in, ksize);
                auto vsize = read_simple<uint32_t>(in);
                auto value = atomic_cell_view::from_bytes(type_info, read_simple_bytes(in, vsize));
                ret.cells.emplace_back(key, value);
            }
        },
        [&] (const user_type_impl& utype) {
            for (uint32_t i = 0; i != nr; ++i) {
                auto ksize = read_simple<uint32_t>(in);
                auto key = read_simple_bytes(in, ksize);
                auto vsize = read_simple<uint32_t>(in);
                auto value = atomic_cell_view::from_bytes(
                        utype.type(deserialize_field_index(key))->imr_state().type_info(), read_simple_bytes(in, vsize));
                ret.cells.emplace_back(key, value);
            }
        },
        [&] (const abstract_type& o) {
            throw std::runtime_error(format("deserialize_collection_mutation: unknown type {}", o.name()));
        }
    ));

    assert(in.empty());
    return ret;
}
