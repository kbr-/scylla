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
// row_tombstone
#include "mutation_partition.hh"
#include "compaction_garbage_collector.hh"
#include "combine.hh"

#include "multi_cell_mutation.hh"

static collection_mutation_helper materialize(const collection_mutation_view_helper& cmv, const collection_type_impl& ctype) {
    collection_mutation_helper m;
    m.tomb = cmv.tomb;
    m.cells.reserve(cmv.cells.size());
    for (auto&& e : cmv.cells) {
        m.cells.emplace_back(bytes(e.first.begin(), e.first.end()), atomic_cell(*ctype.value_comparator(), e.second));
    }
    return m;
}

static collection_mutation_helper materialize(const collection_mutation_view_helper& cmv, const user_type_impl& utype) {
    collection_mutation_helper m;
    m.tomb = cmv.tomb;
    m.cells.reserve(cmv.cells.size());
    // delete b from a.ts where a=1;
    // select * from a.ts where a=1;
    // scylla: multi_cell_mutation.cc:45: collection_mutation_helper materialize(const collection_mutation_view_helper&, const user_type_impl&): Assertion `cmv.cells.size() == utype.size()' failed.
    assert(cmv.cells.size() <= utype.size());
    for (auto&& e : cmv.cells) {
        m.cells.emplace_back(bytes(e.first.begin(), e.first.end()),
                atomic_cell(*utype.type(deserialize_field_index(e.first)), e.second));
    }
    return m;
}

collection_mutation_helper collection_mutation_view_helper::materialize(const data_type& typ) const {
    // TODO FIXME kbr
    auto ctype = dynamic_cast<const collection_type_impl*>(typ.get());
    auto utype = dynamic_cast<const user_type_impl*>(typ.get());
    assert(ctype || utype);

    return ctype ? ::materialize(*this, *ctype) : ::materialize(*this, *utype);
}

bool collection_mutation_view::is_empty() const {
  return data.with_linearized([&] (bytes_view in) { // FIXME: we can guarantee that this is in the first fragment
    auto has_tomb = read_simple<bool>(in);
    return !has_tomb && read_simple<uint32_t>(in) == 0;
  });
}

template <typename F>
static bool is_any_live(const collection_mutation_view& cm, F&& get_cell_type, tombstone tomb, gc_clock::time_point tp) {
  return cm.data.with_linearized([&] (bytes_view in) {
    auto has_tomb = read_simple<bool>(in);
    if (has_tomb) {
        auto ts = read_simple<api::timestamp_type>(in);
        auto ttl = read_simple<gc_clock::duration::rep>(in);

        tomb.apply(tombstone{ts, gc_clock::time_point(gc_clock::duration(ttl))});
    }
    auto nr = read_simple<uint32_t>(in);
    for (uint32_t i = 0; i != nr; ++i) {
        auto ksize = read_simple<uint32_t>(in);
        in.remove_prefix(ksize);
        auto vsize = read_simple<uint32_t>(in);
        //auto value = atomic_cell_view::from_bytes(value_comparator()->imr_state().type_info(), read_simple_bytes(in, vsize));
        // TODO kbr forward needed?
        auto value = atomic_cell_view::from_bytes(
                get_cell_type(i)->imr_state().type_info(), read_simple_bytes(in, vsize));
        if (value.is_live(tomb, tp, false)) {
            return true;
        }
    }
    return false;
  });
}

bool collection_mutation_view::is_any_live(const data_type& typ, tombstone tomb, gc_clock::time_point tp) const {
    // TODO FIXME kbr
    auto ctype = dynamic_cast<const collection_type_impl*>(typ.get());
    auto utype = dynamic_cast<const user_type_impl*>(typ.get());
    assert(ctype || utype);

    return ctype
        ? ::is_any_live(*this, [ctype] (uint32_t) { return ctype->value_comparator(); }, tomb, tp)
        : ::is_any_live(*this, [utype] (uint32_t i) { return utype->type(i); }, tomb, tp);
}

// TODO kbr copy paste... similar algorithm
// somehow use if constexpr?
api::timestamp_type collection_mutation_view::last_update(const data_type& typ) const {
    // TODO FIXME kbr
    auto ctype = dynamic_cast<const collection_type_impl*>(typ.get());
    assert(ctype);
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
            auto value = atomic_cell_view::from_bytes(ctype->value_comparator()->imr_state().type_info(), read_simple_bytes(in, vsize));
            max = std::max(value.timestamp(), max);
        }
        return max;
    });
}

bool collection_mutation_helper::compact_and_expire(column_id id, row_tombstone base_tomb, gc_clock::time_point query_time,
        can_gc_fn& can_gc, gc_clock::time_point gc_before, compaction_garbage_collector* collector) {
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
        collector->collect(id, collection_mutation_helper{purged_tomb, std::move(losers)});
    }
    cells = std::move(survivors);
    return any_live;
}

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

template <typename Iterator>
collection_mutation do_serialize_collection_mutation(
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

collection_mutation serialize_collection_mutation(const data_type& typ, const collection_mutation_helper& mut) {
    return do_serialize_collection_mutation(*typ, mut.tomb, boost::make_iterator_range(mut.cells.begin(), mut.cells.end()));
}

collection_mutation serialize_collection_mutation(const data_type& typ, const collection_mutation_view_helper& mut) {
    return do_serialize_collection_mutation(*typ, mut.tomb, boost::make_iterator_range(mut.cells.begin(), mut.cells.end()));
}

template <typename Compare>
static collection_mutation_view_helper merge(Compare&& cmp, collection_mutation_view_helper a, collection_mutation_view_helper b) {
    using element_type = std::pair<bytes_view, atomic_cell_view>;

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

    collection_mutation_view_helper merged;
    merged.cells.reserve(a.cells.size() + b.cells.size());
    combine(a.cells.begin(), std::remove_if(a.cells.begin(), a.cells.end(), cell_killed(b.tomb)),
            b.cells.begin(), std::remove_if(b.cells.begin(), b.cells.end(), cell_killed(a.tomb)),
            std::back_inserter(merged.cells),
            std::forward<Compare>(cmp),
            merge);
    merged.tomb = std::max(a.tomb, b.tomb);

    return merged;
}

collection_mutation merge(const data_type& typ, collection_mutation_view a, collection_mutation_view b) {
    // TODO kbr: get rid of multiple dispatches
    return a.with_deserialized_view(typ, [&] (collection_mutation_view_helper aview) {
        return b.with_deserialized_view(typ, [&] (collection_mutation_view_helper bview) {
            // TODO kbr
            auto ct = dynamic_pointer_cast<const collection_type_impl>(typ);
            assert(ct || dynamic_pointer_cast<const user_type_impl>(typ));

            using element_type = std::pair<bytes_view, atomic_cell_view>;

            collection_mutation_view_helper merged;

            if (ct) {
                auto key_type = ct->name_comparator();
                auto compare = [key_type = std::move(key_type)] (const element_type& e1, const element_type& e2) {
                    return key_type->less(e1.first, e2.first);
                };

                merged = merge(std::move(compare), std::move(aview), std::move(bview));
            } else {
                auto compare = [] (const element_type& e1, const element_type& e2) {
                    return deserialize_field_index(e1.first) < deserialize_field_index(e2.first);
                };

                merged = merge(std::move(compare), std::move(aview), std::move(bview));
            }

            return serialize_collection_mutation(typ, std::move(merged));
        });
    });


    /*

    delete b from a.ts where a=1;
    select * from a.ts where a=1;\
    scylla: multi_cell_mutation.cc:256: merge(const data_type&, collection_mutation_view, collection_mutation_view)::<lambda(collection_mutation_view_helper)>::<lambda(collection_mutation_view_helper)>: Assertion `ct' failed.

    auto key_type = ct->name_comparator();

serializing cell key for list:
    auto uuid1 = utils::UUID_gen::get_time_UUID_bytes();
    auto uuid = bytes(reinterpret_cast<const int8_t*>(uuid1.data()), uuid1.size());

serializing cell key for udt:
    bytes idx_buf(bytes::initialized_later(), sizeof(uint16_t));
    *reinterpret_cast<uint16_t*>(idx_buf.begin()) = (uint16_t)net::hton(idx);

deserializing cell key for udt:
    uint16_t field_pos = net::ntoh(*reinterpret_cast<const uint16_t*>(e.first.begin()));

data_type
list_type_impl::name_comparator() const {
    return timeuuid_type;
}

map:
    virtual data_type name_comparator() const override { return _keys; }

set:
    virtual data_type name_comparator() const override { return _elements; }

could use integer_type_impl<uint32_t> for (de)serializing and comparison?
    (de)serialization: (de)compose_value

     */
}

collection_mutation difference(const data_type& typ, collection_mutation_view a, collection_mutation_view b) {
    // TODO kbr two dispatches...
    return a.with_deserialized_view(typ, [&] (collection_mutation_view_helper aview) {
    return b.with_deserialized_view(typ, [&] (collection_mutation_view_helper bview) {

    // TODO FIXME kbr
    auto ct = dynamic_pointer_cast<const collection_type_impl>(typ);
    assert(ct);

    auto key_type = ct->name_comparator();

    collection_mutation_view_helper diff;
    diff.cells.reserve(std::max(aview.cells.size(), bview.cells.size()));

    auto it = bview.cells.begin();
    for (auto&& c : aview.cells) {
        while (it != bview.cells.end() && key_type->less(it->first, c.first)) {
            ++it;
        }
        if (it == bview.cells.end() || !key_type->equal(it->first, c.first)
            || compare_atomic_cell_for_merge(c.second, it->second) > 0) {

            auto cell = std::make_pair(c.first, c.second);
            diff.cells.emplace_back(std::move(cell));
        }
    }
    if (aview.tomb > bview.tomb) {
        diff.tomb = aview.tomb;
    }

    return serialize_collection_mutation(typ, std::move(diff));

    });});
}

template <typename F>
static collection_mutation_view_helper deserialize_collection_mutation(F&& get_cell_type, bytes_view in) {
    collection_mutation_view_helper ret;

    auto has_tomb = read_simple<bool>(in);
    if (has_tomb) {
        auto ts = read_simple<api::timestamp_type>(in);
        auto ttl = read_simple<gc_clock::duration::rep>(in);
        ret.tomb = tombstone{ts, gc_clock::time_point(gc_clock::duration(ttl))};
    }

    auto nr = read_simple<uint32_t>(in);
    ret.cells.reserve(nr);
    for (uint32_t i = 0; i != nr; ++i) {
        // FIXME: we could probably avoid the need for size
        // TODO kbr: fix above fixme. Can use type to dermine size (collection: name_comparator, udt: sizeof(uint16_t))
        auto ksize = read_simple<uint32_t>(in);
        auto key = read_simple_bytes(in, ksize);
        auto vsize = read_simple<uint32_t>(in);
        // TODO kbr forward needed?
        auto value = atomic_cell_view::from_bytes(
                get_cell_type(key)->imr_state().type_info(), read_simple_bytes(in, vsize));
        ret.cells.emplace_back(key, value);
    }

    assert(in.empty());
    return ret;
}

collection_mutation_view_helper deserialize_collection_mutation(const data_type& typ, bytes_view in) {
    // TODO FIXME kbr
    auto ctype = dynamic_cast<const collection_type_impl*>(typ.get());
    auto utype = dynamic_cast<const user_type_impl*>(typ.get());
    assert(ctype || utype);

    return ctype
        ? deserialize_collection_mutation([ctype] (const bytes_view&) { return ctype->value_comparator(); }, in)
        : deserialize_collection_mutation([utype] (const bytes_view& b) {
                return utype->type(deserialize_field_index(b));
          }, in);
}
