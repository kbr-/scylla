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

#include <boost/type.hpp>
#include <random>
#include <unordered_set>
#include <seastar/core/sleep.hh>

#include "keys.hh"
#include "schema_builder.hh"
#include "db/config.hh"
#include "db/system_keyspace.hh"
#include "db/system_distributed_keyspace.hh"
#include "dht/token-sharding.hh"
#include "locator/token_metadata.hh"
#include "gms/application_state.hh"
#include "gms/inet_address.hh"
#include "gms/gossiper.hh"

#include "cdc/generation.hh"

extern logging::logger cdc_log;

struct sharding_info {
    int shard_count;
    int ignore_msb;
};

static int get_shard_count(const gms::inet_address& endpoint, const gms::gossiper& g) {
    auto ep_state = g.get_application_state_ptr(endpoint, gms::application_state::SHARD_COUNT);
    return ep_state ? std::stoi(ep_state->value) : -1;
}

static unsigned get_sharding_ignore_msb(const gms::inet_address& endpoint, const gms::gossiper& g) {
    auto ep_state = g.get_application_state_ptr(endpoint, gms::application_state::IGNORE_MSB_BITS);
    return ep_state ? std::stoi(ep_state->value) : 0;
}

static std::optional<sharding_info> get_sharding_info(const gms::inet_address& endpoint, const gms::gossiper& g) {
    //  TODO
}

static sharding_info get_my_sharding_info(const db::config& cfg) {
    return {smp::count, cfg.murmur3_partitioner_ignore_msb_bits()};
}

static std::unordered_set<dht::token> get_tokens(const gms::inet_address& ep, const gms::gossiper& g) {
    return gms::versioned_value::tokens_from_string(g.get_application_state_value(ep, gms::application_state::TOKENS));
}

namespace cdc {

extern const api::timestamp_clock::duration generation_leeway =
    std::chrono::duration_cast<api::timestamp_clock::duration>(std::chrono::seconds(5));

static void copy_int_to_bytes(int64_t i, size_t offset, bytes& b) {
    i = net::hton(i);
    std::copy_n(reinterpret_cast<int8_t*>(&i), sizeof(int64_t), b.begin() + offset);
}

stream_id::stream_id(int64_t first, int64_t second)
    : _value(bytes::initialized_later(), 2 * sizeof(int64_t))
{
    copy_int_to_bytes(first, 0, _value);
    copy_int_to_bytes(second, sizeof(int64_t), _value);
}

stream_id::stream_id(bytes b) : _value(std::move(b)) { }

bool stream_id::is_set() const {
    return !_value.empty();
}

bool stream_id::operator==(const stream_id& o) const {
    return _value == o._value;
}

bool stream_id::operator<(const stream_id& o) const {
    return _value < o._value;
}

static int64_t bytes_to_int64(const bytes& b, size_t offset) {
    assert(b.size() >= offset + sizeof(int64_t));
    int64_t res;
    std::copy_n(b.begin() + offset, sizeof(int64_t), reinterpret_cast<int8_t *>(&res));
    return net::ntoh(res);
}

int64_t stream_id::first() const {
    return bytes_to_int64(_value, 0);
}

int64_t stream_id::second() const {
    return bytes_to_int64(_value, sizeof(int64_t));
}

const bytes& stream_id::to_bytes() const {
    return _value;
}

partition_key stream_id::to_partition_key(const schema& log_schema) const {
    return partition_key::from_single_value(log_schema, _value);
}

bool token_range_description::operator==(const token_range_description& o) const {
    return token_range_end == o.token_range_end && streams == o.streams
        && sharding_ignore_msb == o.sharding_ignore_msb;
}

topology_description::topology_description(std::vector<token_range_description> entries)
    : _entries(std::move(entries)) {}

bool topology_description::operator==(const topology_description& o) const {
    return _entries == o._entries;
}

const std::vector<token_range_description>& topology_description::entries() const {
    return _entries;
}

static stream_id make_random_stream_id() {
    static thread_local std::mt19937_64 rand_gen(std::random_device().operator()());
    static thread_local std::uniform_int_distribution<int64_t> rand_dist(std::numeric_limits<int64_t>::min());

    return {rand_dist(rand_gen), rand_dist(rand_gen)};
}

topology_description generate_topology_description(
        std::map<dht::token, sharding_info> token_to_sharding_info) {
}

/* Given:
 * 1. a set of tokens which split the token ring into token ranges (vnodes),
 * 2. information on how each token range is distributed among its owning node's shards
 * this function tries to generate a set of CDC stream identifiers such that for each
 * shard and vnode pair there exists a stream whose token falls into this
 * vnode and is owned by this shard.
 *
 * It then builds a cdc::topology_description which maps tokens to these
 * found stream identifiers, such that if token T is owned by shard S in vnode V,
 * it gets mapped to the stream identifier generated for (S, V).
 */
// Run in seastar::async context.
topology_description generate_topology_description(
        const db::config& cfg,
        const std::unordered_set<dht::token>& bootstrap_tokens,
        const locator::token_metadata& token_metadata,
        const gms::gossiper& gossiper) {
    if (bootstrap_tokens.empty()) {
        throw std::runtime_error(
                "cdc: bootstrap tokens is empty in generate_topology_description");
    }

    auto tokens = token_metadata.sorted_tokens();
    tokens.insert(tokens.end(), bootstrap_tokens.begin(), bootstrap_tokens.end());
    std::sort(tokens.begin(), tokens.end());
    tokens.erase(std::unique(tokens.begin(), tokens.end()), tokens.end());

    std::vector<token_range_description> entries(tokens.size());
    int spots_to_fill = 0;

    for (size_t i = 0; i < tokens.size(); ++i) {
        auto& entry = entries[i];
        entry.token_range_end = tokens[i];

        if (bootstrap_tokens.count(entry.token_range_end) > 0) {
            entry.streams.resize(smp::count);
            entry.sharding_ignore_msb = cfg.murmur3_partitioner_ignore_msb_bits();
        } else {
            auto endpoint = token_metadata.get_endpoint(entry.token_range_end);
            if (!endpoint) {
                throw std::runtime_error(format("Can't find endpoint for token {}", entry.token_range_end));
            }
            auto sc = get_shard_count(*endpoint, gossiper);
            entry.streams.resize(sc > 0 ? sc : 1);
            entry.sharding_ignore_msb = get_sharding_ignore_msb(*endpoint, gossiper);
        }

        spots_to_fill += entry.streams.size();
    }

    auto schema = schema_builder("fake_ks", "fake_table")
        .with_column("stream_id", bytes_type, column_kind::partition_key)
        .build();

    auto quota = std::chrono::seconds(spots_to_fill / 2000 + 1);
    auto start_time = std::chrono::system_clock::now();

    // For each pair (i, j), 0 <= i < streams.size(), 0 <= j < streams[i].size(),
    // try to find a stream (stream[i][j]) such that the token of this stream will get mapped to this stream
    // (refer to the comments above topology_description's definition to understand how it describes the mapping).
    // We find the streams by randomly generating them and checking into which pairs they get mapped.
    // NOTE: this algorithm is temporary and will be replaced after per-table-partitioner feature gets merged in.
    repeat([&] {
        for (int i = 0; i < 500; ++i) {
            auto stream_id = make_random_stream_id();
            auto token = dht::get_token(*schema, stream_id.to_partition_key(*schema));

            // Find the token range into which our stream_id's token landed.
            auto it = std::lower_bound(tokens.begin(), tokens.end(), token);
            auto& entry = entries[it != tokens.end() ? std::distance(tokens.begin(), it) : 0];

            auto shard_id = dht::shard_of(entry.streams.size(), entry.sharding_ignore_msb, token);
            assert(shard_id < entry.streams.size());

            if (!entry.streams[shard_id].is_set()) {
                --spots_to_fill;
                entry.streams[shard_id] = stream_id;
            }
        }

        if (!spots_to_fill) {
            return stop_iteration::yes;
        }

        auto now = std::chrono::system_clock::now();
        auto passed = std::chrono::duration_cast<std::chrono::seconds>(now - start_time);
        if (passed > quota) {
            return stop_iteration::yes;
        }

        return stop_iteration::no;
    }).get();

    if (spots_to_fill) {
        // We were not able to generate stream ids for each (token range, shard) pair.

        // For each range that has a stream, for each shard for this range that doesn't have a stream,
        // use the stream id of the next shard for this range.

        // For each range that doesn't have any stream,
        // use streams of the first range to the left which does have a stream.

        cdc_log.warn("Generation of CDC streams failed to create streams for some (vnode, shard) pair."
                     " This can lead to worse performance.");

        stream_id some_stream;
        size_t idx = 0;
        for (; idx < entries.size(); ++idx) {
            for (auto s: entries[idx].streams) {
                if (s.is_set()) {
                    some_stream = s;
                    break;
                }
            }
            if (some_stream.is_set()) {
                break;
            }
        }

        assert(idx != entries.size() && some_stream.is_set());

        // Iterate over all ranges in the clockwise direction, starting with the one we found a stream for.
        for (size_t off = 0; off < entries.size(); ++off) {
            auto& ss = entries[(idx + off) % entries.size()].streams;

            int last_set_stream_idx = ss.size() - 1;
            while (last_set_stream_idx > -1 && !ss[last_set_stream_idx].is_set()) {
                --last_set_stream_idx;
            }

            if (last_set_stream_idx == -1) {
                cdc_log.warn(
                        "CDC wasn't able to generate any stream for vnode ({}, {}]. We'll use another vnode's streams"
                        " instead. This might lead to inconsistencies.",
                        tokens[(idx + off + entries.size() - 1) % entries.size()], tokens[(idx + off) % entries.size()]);

                ss[0] = some_stream;
                last_set_stream_idx = 0;
            }

            some_stream = ss[last_set_stream_idx];

            // Replace 'unset' stream ids with indexes below last_set_stream_idx
            for (int s_idx = last_set_stream_idx - 1; s_idx > -1; --s_idx) {
                if (ss[s_idx].is_set()) {
                    some_stream = ss[s_idx];
                } else {
                    ss[s_idx] = some_stream;
                }
            }
            // Replace 'unset' stream ids with indexes above last_set_stream_idx
            for (int s_idx = ss.size() - 1; s_idx > last_set_stream_idx; --s_idx) {
                if (ss[s_idx].is_set()) {
                    some_stream = ss[s_idx];
                } else {
                    ss[s_idx] = some_stream;
                }
            }
        }
    }

    return {std::move(entries)};
}

bool should_propose_first_generation(const gms::inet_address& me, const gms::gossiper& g) {
    auto my_host_id = g.get_host_id(me);
    auto& eps = g.get_endpoint_states();
    return std::none_of(eps.begin(), eps.end(),
            [&] (const std::pair<gms::inet_address, gms::endpoint_state>& ep) {
        return my_host_id < g.get_host_id(ep.first);
    });
}

future<db_clock::time_point> get_local_streams_timestamp() {
    return db::system_keyspace::get_saved_cdc_streams_timestamp().then([] (std::optional<db_clock::time_point> ts) {
        if (!ts) {
            auto err = format("get_local_streams_timestamp: tried to retrieve streams timestamp after bootstrapping, but it's not present");
            cdc_log.error("{}", err);
            throw std::runtime_error(err);
        }
        return *ts;
    });
}

// Run inside seastar::async context.
db_clock::time_point make_new_cdc_generation(
        const db::config& cfg,
        const std::unordered_set<dht::token>& bootstrap_tokens,
        const locator::token_metadata& tm,
        const gms::gossiper& g,
        db::system_distributed_keyspace& sys_dist_ks,
        std::chrono::milliseconds ring_delay,
        bool for_testing) {
    assert(!bootstrap_tokens.empty());

    auto gen = generate_topology_description(cfg, bootstrap_tokens, tm, g);

    // Begin the race.
    auto ts = db_clock::now() + (
            for_testing ? std::chrono::milliseconds(0) : (
                2 * ring_delay + std::chrono::duration_cast<std::chrono::milliseconds>(generation_leeway)));
    sys_dist_ks.insert_cdc_topology_description(ts, std::move(gen), { tm.count_normal_token_owners() }).get();

    return ts;
}

std::optional<db_clock::time_point> get_streams_timestamp_for(const gms::inet_address& endpoint, const gms::gossiper& g) {
    auto streams_ts_string = g.get_application_state_value(endpoint, gms::application_state::CDC_STREAMS_TIMESTAMP);
    cdc_log.trace("endpoint={}, streams_ts_string={}", endpoint, streams_ts_string);
    return gms::versioned_value::cdc_streams_timestamp_from_string(streams_ts_string);
}

// Run inside seastar::async context.
static void do_update_streams_description(
        db_clock::time_point streams_ts,
        db::system_distributed_keyspace& sys_dist_ks,
        db::system_distributed_keyspace::context ctx) {
    if (sys_dist_ks.cdc_desc_exists(streams_ts, ctx).get0()) {
        cdc_log.debug("update_streams_description: description of generation {} already inserted", streams_ts);
        return;
    }

    // We might race with another node also inserting the description, but that's ok. It's an idempotent operation.

    auto topo = sys_dist_ks.read_cdc_topology_description(streams_ts, ctx).get0();
    if (!topo) {
        throw std::runtime_error(format("could not find streams data for timestamp {}", streams_ts));
    }

    std::set<cdc::stream_id> streams_set;
    for (auto& entry: topo->entries()) {
        streams_set.insert(entry.streams.begin(), entry.streams.end());
    }

    std::vector<cdc::stream_id> streams_vec(streams_set.begin(), streams_set.end());

    sys_dist_ks.create_cdc_desc(streams_ts, streams_vec, ctx).get();
    cdc_log.info("CDC description table successfully updated with generation {}.", streams_ts);
}

void update_streams_description(
        db_clock::time_point streams_ts,
        shared_ptr<db::system_distributed_keyspace> sys_dist_ks,
        noncopyable_function<unsigned()> get_num_token_owners,
        abort_source& abort_src) {
    try {
        do_update_streams_description(streams_ts, *sys_dist_ks, { get_num_token_owners() });
    } catch(...) {
        cdc_log.warn(
            "Could not update CDC description table with generation {}: {}. Will retry in the background.",
            streams_ts, std::current_exception());

        // It is safe to discard this future: we keep system distributed keyspace alive.
        (void)seastar::async([
            streams_ts, sys_dist_ks, get_num_token_owners = std::move(get_num_token_owners), &abort_src
        ] {
            while (true) {
                sleep_abortable(std::chrono::seconds(60), abort_src).get();
                try {
                    do_update_streams_description(streams_ts, *sys_dist_ks, { get_num_token_owners() });
                    return;
                } catch (...) {
                    cdc_log.warn(
                        "Could not update CDC description table with generation {}: {}. Will try again.",
                        streams_ts, std::current_exception());
                }
            }
        });
    }
}

static void assert_shard_zero(const sstring& where) {
    if (this_shard_id() != 0) {
        on_internal_error(format("`{}`: must be run on shard 0", where));
    }
}

/* When a node sees other node gossiping a CDC generation timestamp that it doesn't know,
 * it requests the gossiping node to send it the CDC generation. */
future<cdc::topology_description> handle_request_generation(db_clock::time_point gen_timestamp) {
    assert_shard_zero(__PRETTY_FUNCTION__); // TODO is this really required?

    return db::system_keyspace::load_cdc_generation(gen_timestamp).then(
            [] (std::optional<cdc::topology_description> desc) {
        if (!desc) {
            // TODO: this must be a bug, explain.
            // but we need the "expired" column so we don't GC a generation that we've been gossiping
            // one second ago (because we just restarted and learned there's a new gen for a week, say).
            throw std::runtime_error("requested a gen we don't know about");
            // TODO: return an error RPC so we don't crash, the requester can simply request another node.
            // (or he can crash)
            // does `throw` work over RPC?
        }

        return std::move(*desc);
    });
}

struct promise {
    std::optional<db_clock::time_point> ts;
    std::vector<inet_address> known; // sorted vector of nodes
};

// Run inside seastar::async.
static std::unordered_set<dht::token> wait_for_tokens(
        const std::unordered_set<inet_address> nodes,
        /* TODO timeout */) {
    // TODO
    // TODO pass gossiper, PEERs?
    // wait until the tokens of all `including_nodes` show up in gossip
    // and return the tokens of all seen nodes
    // or timeout
    while (true) {
        if (std::any_of(nodes.begin(), nodes.end(), [] (const inet_address& s) {
                    // TODO catch gossiper
            return g.get_application_state_value(s, gms::application_state::TOKENS).empty();
        })) {
            // TODO log
            // TODO timeout
            // TODO pass abort source
            sleep_abortable(std::chrono::seconds(1), as).get();
            continue;
        }

        // All tokens have appeared in gossip

        std::unordered_set<dht::token> ret;
        for (auto& e: g.get_endpoint_states()) {
            ret.merge(get_tokens(e.first, g));
        }
        return ret;
    }

    throw "fix your network bro"; // TODO

}

// Run in async
static void generation_service::prepare_cdc_metadata(db_clock::time_point ts) {
    assert_shard_zero(__PRETTY_FUNCTION__); // TODO are we sure?

    container().invoke_on_all([ts] (generation_service& svc) {
        svc._metadata.prepare(ts);
    }).get();
}

// Run in async
static void generation_service::update_cdc_metadata(db_clock::time_point ts, topology_description desc) {
    assert_shard_zero(__PRETTY_FUNCTION__); // TODO are we sure?

    container().invoke_on_others([ts, desc] (generation_service& svc) mutable {
            svc._metadata.insert(ts, std::move(desc));
    }).get();

    svc._metadata.insert(ts, std::move(desc));
}

// Run in async
static void generation_service::record_generation(db_clock::time_point ts, topology_description desc) {
    assert_shard_zero(__PRETTY_FUNCTION__); // TODO are we sure?

    db::system_keyspace::save_cdc_generation(ts, std::move(desc)).get();
    _stored_gen_tss.insert(ts);
}

//static void wait_for_timestamp(db_clock::time_point min_ts) {
//    // TODO
//    // wait until everyone gossips a timestamp >= min_ts
//    // or timeout
//}

// join token ring means we didn't join the token ring yet.
// If we are restarting, we're NOT joining token ring. We already joined it.
void generation_service::before_join_token_ring() {
    using namespace std::chrono;
    assert_shard_zero(__PRETTY_FUNCTION__);
        // Even if we reached this point before but crashed, we will make a new CDC generation.
        // It doesn't hurt: other nodes will (potentially) just do more generation switches.
    // TODO: pass bootstrap tokens
    // TODO: pass seed list
    // TODO: pass gossiper by param instead?
    // TODO: abort_source, pass it everywhere
    // TODO: for_testing
    // TODO: ring_delay
    // TODO: pass _current_gen_timestamp
    // TODO: pass _nodes_promised_to
    // TODO: handle the restarting case differently
    //          pass bool is_restarting? or make a separate function
    //          handle system.peers
    // TODO: think about node replacement
    //          we can always create a new generation, so we can use the standard path...
    // TODO: rolling upgrade
    // TODO: if bootstrap complete, do nothing?
    // TODO: ensure bootstrap_tokens persisted
    // TODO if other side doesn't know rpc verb
    // cancel this algorithm and join without starting new cdc generation
    // TODO persist _known in proper places (before sending a response to promise!)

    // These shall be my tokens, but don't insert them into your token rings yet.
    _gossiper.add_local_application_state({
        { gms::application_state::TOKENS, versioned_value::tokens(bootstrap_tokens) },
        { gms::application_state::STATUS, versioned_value::announcing_tokens(bootstrap_tokens) }
    }).get();

    // TODO fix variables
    _known.insert(seeds.begin(), seeds.end());
    for (auto& e: g.get_endpoint_states()) {
        _known.insert(e.first);
    }

    std::unordered_set<gms::inet_address> contacted;
    std::optional<db_clock::time_point> max_known_ts;
    while (contacted.size() < _known.size()) {
        std::vector missing;
        for (auto& n: _known) {
            if (!contacted.count(n)) {
                missing.push_back(n);
            }
        }

        // TODO timeout etc.
        auto responses = parallel_for_each(missing.begin(), missing.end(), [this] (inet_address ep) {
            return _ms.send_cdc_request_promise(netw::messaging_service::msg_addr{ep, 0},
                    // timeout
            );
        }).get0();

        for (auto& msg: responses) {
            _known.insert(msg.known.begin(), msg.known_end());

            if (!max_known_ts || (msg.ts && *msg.ts > *max_known_ts)) {
                max_known_ts = msg.ts;
            }
        }
        contacted.insert(missing.begin(), missing.end());
    }

    // For propagating our generation, we will depend on the ring_delay assumption:
    // anything inserted into gossip will arrive at other nodes within ring_delay.
    // We're adding 2 * ring_delay because TODO
    auto chosen_ts = db_clock::now() + (
            for_testing ? milliseconds(0) : (2 * ring_delay + duration_cast<milliseconds>(generation_leeway));
    if (max_known_ts) {
        chosen_ts = std::max(chosen_ts, *max_known_ts + seconds(1));
    }

    // _current_ts is the highest observed generation timestamp.
    if (_current_ts && *_current_ts > chosen_ts) {
        // At this point we've already seen a higher-timestamped generation in gossip
        // and we're already propagating it to other nodes.
        // Thus our generation won't ever be used by anyone so we can optimize and decide to not create it at all.
        return;
    }

    // We're setting _current_ts here to correctly answer to promise requests,
    // but we don't set it in Gossip yet, since we don't have the topology description of this generation.
    // It will be set in `record_generation` below.
    _current_ts = chosen_ts;

    // Wait for at most ring_delay, so we still have ring_delay to gossip the timestamp.
    // TODO wait for less...
    auto tokens = wait_for_tokens(_known, /* TODO now + */ ring_delay);

    if (*_current_ts > chosen_ts) {
        // Same optimization as above.
        return;
    }

    // TODO fix generate_topology_description call
    auto gen_desc = generate_topology_description(std::move(tokens));

    // Save this generation and start gossiping its timestamp.
    record_generation(chosen_ts, gen_desc);

    gossip_generation_timestamp(chosen_ts).get();

    update_cdc_metadata(chosen_ts, std::move(gen_desc));

    // At this point we are gossiping a timestamp >= chosen_ts.
    // Wait until everyone is gossiping a timestamp >= chosen_ts before proceeding.
    // This ensures that our generation will be known before we go into NORMAL status;
    // we can then safely crash after going into NORMAL.
    // TODO: controversial?
    // TODO I want to check that my timestamp arrived, not that >= timestamp arrived
    wait_for_timestamp(chosen_ts);
}

void generation_service::before_rejoin(
        const db::config& cfg
        ) {
    assert_shard_zero(__PRETTY_FUNCTION__); // TODO are we sure?

    // TODO scan gossiper for generations
    for (auto& e: _gossiper.get_endpoint_states()) {
        _gossiper.get_application_state_value(e.first, gms::application_state::CDC_STREAMS_TIMESTAMP);
    }

    //auto gen = db::system_keyspace::load_latest_cdc_generation();
    //if (gen) {
    //    auto ts = *gen.first;
    //    gossip_generation_timestamp(*gen.first);
    //    // TODO: _metadata
    //    return;
    //}

    // TODO: log a warning
            // cdc_log.warn(
            //         "Restarting node in NORMAL status with CDC enabled, but no streams timestamp was proposed"
            //         " by this node according to its local tables. Are we upgrading from a non-CDC supported version?");

    // No CDC generations are present in our local tables, which means one of two things:
    // - we're performing a rolling upgrade, or
    // - we've lost everything due to a byzantine failure, and didn't manage to retrieve
    //   any generation from other nodes in the background before this function was called.
    //   Well, it's weird that the failure managed to delete the CDC-related tables but not the
    //   bootstrap_complete flag (which had to be present since we were put on the restarting path
    //   and entered this function). We can probably assume that such specifically-CDC-targeted
    //   failures won't happen, but if they do, we'll just act as if rolling upgrade is happening.

    // We're going to create a new generation. As explained above, most probably we're performing
    // a rolling upgrade and that this is one of the first nodes to upgrade. Later nodes will
    // see a generation in gossip (maybe ours) when entering `on_restart()` so not too many
    // reduntant generations should appear due to this process.

    std::map<dht::token, sharding_info> tok_to_sinfo;
    for (auto& e: _gossiper.get_endpoint_states()) {
        auto ep = e.first;
        auto tokens = get_tokens(ep, _gossiper);
        auto sinfo = get_sharding_info(ep, _gossiper);
        if (!sinfo) {
            // If the sharding info for that node is not present in Gossiper, we will use our sharding info.
            // This will work well in homogeneous clusters that have the same sharding parameters across all nodes.

            // For non-homogeneous clusters this might lead to suboptimal CDC generation in terms of shard balance,
            // but node colocation will still be preserved, so it's not a correctness issue.

            // In any case, sharding info missing from Gossiper should happen extremely rarely,
            // only when:
            // - the user is performing a rolling upgrade non-serially by first shutting down the entire cluster
            //   (which he should not do), so that nodes have lost the sharding info of other nodes
            //    and didn't receive it back because those nodes are still down,
            // - or if the cluster is having connectivity issues during rolling upgrade
            //   (which the user should take care not to happen), so that the upgrading restarting node
            //   was unable to contact any other node before entering `on_restart()`.

            // In practice we should never reach this code.

            sinfo = get_my_sharding_info(cfg);
        }

        for (auto t: tokens) {
            tok_to_sinfo.insert(ep, *sinfo);
            // It's not important how we resolve token collisions, really. So we just take the sharding info
            // of the first encountered token (because that's what `map::insert` does).
            //
            // First of all, token collisions should never happen. If tokens are generated randomly,
            // they probably won't.
            //
            // Even if some tokens did collide, it may only result in the wrong choice of sharding info,
            // which would break shard-colocation on some primary replicas, but not node-colocation.
        }
    }

    // We also need to include tokens from system.peers (not all tokens might be in the Gossiper at this point).
    // For owners of these tokens, we may not know the sharding info; we will then use our own.
    // This should not happen if we're performing rolling upgrade serially (restart first node,
    // then restart second node, and so on): the restarted node will contact one of the other nodes
    // and update gossip with the sharding info of all nodes. But in the rare case where the user
    // decided to shoot themselves in the foot, or there's a connectivity issue, using our own sharding
    // info is good enough, as explained above.
    auto persisted_tokens = db::system_keyspace::load_tokens().get0();
    for (auto& e: persisted_tokens) {
        auto ep = e.first;

        auto sinfo = get_sharding_info(ep, _gossiper);
        if (!sinfo) {
            sinfo = get_my_sharding_info(cfg);
        }

        for (auto t: e.second) {
            tok_to_sinfo.insert(ep, *sinfo);
        }
    }

    auto gen_desc = generate_topology_description(tok_to_sinfo);
    auto gen_ts = db_clock::now() + 2 * ring_delay + duration_cast<milliseconds>(generation_leeway);

    // TODO optimize? may have already seen a gen
    record_generation(gen_ts, std::move(gen_desc));
}

future<> generation_service::gossip_generation_timestamp(db_clock::time_point gen_ts) {
    assert_shard_zero(__PRETTY_FUNCTION__); // TODO are we sure?

    return with_semaphore(_gen_timestamp_sem, 1, [this] {
        if (_current_ts > gen_ts) {
            return;
        }

        // TODO also update _metadata. Then update the name of this function.
        // THINK ABOUT IT: we might also want to update _metadata with generations different than the latest one.

        _current_ts = gen_ts;
        return _gossiper.add_local_application_state(
                gms::application_state::CDC_STREAMS_TIMESTAMP, versioned_value::cdc_streams_timestamp(gen_ts)
        );
    });
}

void generation_service::on_change(inet_address ep, application_state state, const versioned_value& v) {
    assert_shard_zero(__PRETTY_FUNCTION__);
    // TODO: check if its us
    // TODO: reroute instead? what if gossiper calls us on other shard. Can it do that?
    if (state != application_state::CDC_STREAMS_TIMESTAMP) {
        return;
    }

    auto ts = versioned_value::cdc_streams_timestamp_from_string(v.value);
    cdc_log.debug("Endpoint: {}, CDC generation timestamp change: {}", ep, ts);
    if (!ts) {
        return;
    }

    //TODO: handle somehow the case when the gen is long obsolete?
    // (for example, because this node was down for a long time and restarted)

    // TODO: always prepare _metadata?

    if (_stored_gen_tss.count(*ts)) {
        // _stored_gen_tss has the timestamp => system.cdc_generations also does (invariant I).
        cdc_log.debug("Endpoint: {}, we already know CDC generation timestamp {}", ep, *ts);
        return;
    }

    prepare_cdc_metadata(*ts);

    // Our in-memory cache does not have the timestamp at this moment.
    // system.cdc_generations might already have it, which means that we're racing
    // with the code that updates our cache.
    // It might also not have it, but we might have already requested for that generation
    // in another fiber.
    // But that's OK. Redundant requests don't hurt.
    auto gen_desc = _ms.send_cdc_request_generation(
            netw::messaging_service::msg_addr{ep, 0}, /* TODO timeout */, *ts).get0();
    // TODO: handle the other side throwing.
    // If we cannot throw over RPC, use some other method of sending an error.

    if (_stored_gen_tss.count(*ts)) {
        // A fiber we were racing with has already retrieved it.
        return;
    }

    record_generation(*ts, gen_desc);

    _gossip_timestamp_task = when_all(_gossip_timestamp_task, [this, ts = *ts] {
            // TODO fix vars
            // TODO write big comment
        return sleep_abortable(_abort_source, ring_delay)
                .then([this, ts] { return gossip_generation_timestamp(ts); })
                .handle_exception([] (std::exception_ptr ep) {/* TODO log */ });
    });

    update_cdc_metadata(*ts, std::move(gen_desc));
}

void generation_service::on_start_gossiping(std::map<gms::application_state, gms::versioned_value>& appstates) {
    assert_shard_zero(__PRETTY_FUNCTION__); // are we sure?
    // TODO this is probably not needed. Check what "start_gossiping" nodetool call does
}

generation_service::generation_service(
        netw::messaging_service& ms, gms::gossiper& g, sharded<db::system_distributed_keyspace>& sys_dist_ks)
    : _ms(ms), _gossiper(g), _sys_dist_ks(sys_dist_ks), _gossip_timestamp_task(make_ready_future<>()) {
}

future<> generation_service::start() {
    // Our initialization code requires access to container(), so we have to defer it until after start()...
    // hence generation_service::initialize(), which main must remember to call.

    return make_ready_future<>();
}

// Run in async
future<> generation_service::initialize() {
    assert_shard_zero(__PRETTY_FUNCTION__); // TODO are we sure?
    assert(local_is_initialized());

    // TODO: introduce an unsafe flag that disables this?
    // TODO implement
    _known = db::system_keyspace::cdc_load_known_endpoints().get0();

    // TODO: implement load_cdc_generations, ensure they are stored in increasing timestamp order
    // doesn't matter?
    for (auto& [ts, desc]: db::system_keyspace::load_cdc_generations().get0()) {
        _stored_gen_tss.insert(ts);
        update_cdc_metadata(ts, std::move(desc));
    }

    _ms.register_cdc_request_promise([this] (
            const rpc::client_info& cinfo, rpc::opt_time_point timeout) {
        // TODO: need timeout?
        // TODO: what if we're in the middle of stopping
        // the shard 0 instance might have been stopped already? destroyed?
        return container().invoke_on(0, [from = netw::messaging_service::get_source(cinfo).addr] (generation_service& svc) {
            // TODO document what  this does
            svc._known.insert(from);

            // TODO: we answer with _current_ts, which might be different from our chosen timestamp.
            // However, if we send _current_ts, then _current_ts is >= than our chosen timestamp.
            return promise {svc._current_ts, svc._known};
        });
    });

    _ms.register_cdc_request_generation([this] (
            const rpc::client_info& cinfo, rpc::opt_time_point timeout, db_clock::time_point gen_timestamp) {
        // TODO: need timeout? cinfo?
        return container().invoke_on(0, [gen_timestamp] (generation_service& svc) {
            (void)svc; // TODO: need svc? or use smp::submit_to?
            // capture a shared ptr to qp/db/sp? make them async_sharded_svc?
            return handle_request_generation(gen_timestamp);
        });
    });

    _gossiper.register_(shared_from_this());

    // We might have missed some notifications before subscribing, so scan the gossiper.
}

future<> generation_service::deinitialize() {
    // TODO assert something
    assert_shard_zero(__PRETTY_FUNCTION__); // TODO are we sure?

    // TODO what if only a part of initialize() succeeded?
    // either use some form of RAII or ensure that below ops are no ops
    _gossiper.unregister_(shared_from_this());

    when_all_succeed(
        _ms->unregister_cdc_request_promise(),
        _ms->unregister_cdc_request_generation()
    ).get();

    _gossip_timestamp_task.get();
}

future<> generation_service::stop() {
    // TODO?
}

generation_service::~generation_service() {
    // TODO?
    assert(_stopped);
}

} // namespace cdc
