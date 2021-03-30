/*
 * Copyright (C) 2021 ScyllaDB
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

#include <random>
#include <algorithm>
#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/util/log.hh>
#include <seastar/util/later.hh>
#include <seastar/testing/random.hh>
#include <seastar/testing/thread_test_case.hh>
#include "raft/server.hh"
#include "raft/logical_clock.hh"
#include "serializer.hh"
#include "serializer_impl.hh"
#include "idl/uuid.dist.hh"
#include "idl/uuid.dist.impl.hh"
#include "xx_hasher.hh"

// Test Raft library with declarative test definitions
//
//  Each test can be defined by (struct test_case):
//      .nodes                       number of nodes
//      .total_values                how many entries to append to leader nodes (default 100)
//      .initial_term                initial term # for setup
//      .initial_leader              what server is leader
//      .initial_states              initial logs of servers
//          .le                      log entries
//      .initial_snapshots           snapshots present at initial state for servers
//      .updates                     updates to execute on these servers
//          entries{x}               add the following x entries to the current leader
//          new_leader{x}            elect x as new leader
//          partition{a,b,c}         Only servers a,b,c are connected
//          partition{a,leader{b},c} Only servers a,b,c are connected, and make b leader
//
//      run_test
//      - Creates the servers and initializes logs and snapshots
//        with hasher/digest and tickers to advance servers
//      - Processes updates one by one
//      - Appends remaining values
//      - Waits until all servers have logs of size of total_values entries
//      - Verifies hash
//      - Verifies persisted snapshots
//
//      Tests are run also with 20% random packet drops.
//      Two test cases are created for each with the macro
//          RAFT_TEST_CASE(<test name>, <test case>)

using namespace std::chrono_literals;
using namespace std::placeholders;

static seastar::logger tlogger("test");

lowres_clock::duration tick_delta = 1ms;

std::mt19937 random_generator() {
    auto& gen = seastar::testing::local_random_engine;
    return std::mt19937(gen());
}

int rand() {
    static thread_local std::uniform_int_distribution<int> dist(0, std::numeric_limits<uint8_t>::max());
    static thread_local auto gen = random_generator();

    return dist(gen);
}

// Raft uses UUID 0 as special case.
// Convert local 0-based integer id to raft +1 UUID
utils::UUID to_raft_uuid(size_t local_id) {
    return utils::UUID{0, local_id + 1};
}

raft::server_id to_raft_id(size_t local_id) {
    return raft::server_id{to_raft_uuid(local_id)};
}

class sm_value_impl {
public:
    sm_value_impl() {};
    virtual ~sm_value_impl() {}
    virtual void update(int val) noexcept = 0;
    virtual int64_t get_value() noexcept = 0;
    virtual std::unique_ptr<sm_value_impl> copy() const = 0;
    virtual bool eq(sm_value_impl* o) noexcept {
        return get_value() == o->get_value();
    }
};

class sm_value {
    std::unique_ptr<sm_value_impl> _impl;
public:
    sm_value() {}
    sm_value(std::unique_ptr<sm_value_impl> impl) : _impl(std::move(impl)) {}
    sm_value(sm_value&& o) : _impl(std::move(o._impl)) {}
    sm_value(const sm_value& o) : _impl(o._impl ? o._impl->copy() : nullptr) {}

    void update(int val) {
        _impl->update(val);
    }
    int64_t get_value() {
        return _impl->get_value();
    }
    bool operator==(const sm_value& o) const noexcept {
        return _impl->eq(&*o._impl);
    }
    sm_value& operator=(const sm_value& o) {
        if (o._impl) {
            _impl = o._impl->copy();
        }
        return *this;
    }
};

class hasher_int : public xx_hasher, public sm_value_impl {
public:
    using xx_hasher::xx_hasher;
    void update(int val) noexcept override {
        xx_hasher::update(reinterpret_cast<const char *>(&val), sizeof(val));
    }
    static sm_value value_for(int max) {
        hasher_int h;
        for (int i = 0; i < max; ++i) {
            h.update(i);
        }
        return sm_value(std::make_unique<hasher_int>(std::move(h)));
    }
    int64_t get_value() noexcept override {
        return int64_t(finalize_uint64());
    }
    std::unique_ptr<sm_value_impl> copy() const override {
        return std::make_unique<hasher_int>(*this);
    }
};

class sum_sm : public sm_value_impl {
    int64_t _sum ;
public:
    sum_sm(int64_t sum = 0) : _sum(sum) {}
    void update(int val) noexcept override {
        _sum += val;
    }
    static sm_value value_for(int max) {
        return sm_value(std::make_unique<sum_sm>(((max - 1) * max)/2));
    }
    int64_t get_value() noexcept override {
        return _sum;
    }
    std::unique_ptr<sm_value_impl> copy() const override {
        return std::make_unique<sum_sm>(_sum);
    }
    bool eq(sm_value_impl* o) noexcept override {
        return true;
    }
};

struct snapshot_value {
    sm_value value;
    raft::index_t idx;
};

// Lets assume one snapshot per server
using snapshots = std::unordered_map<raft::server_id, snapshot_value>;
using persisted_snapshots = std::unordered_map<raft::server_id, std::pair<raft::snapshot, snapshot_value>>;

class state_machine : public raft::state_machine {
public:
    using apply_fn = std::function<void(raft::server_id id, const std::vector<raft::command_cref>& commands, sm_value& value)>;
private:
    raft::server_id _id;
    apply_fn _apply;
    size_t _apply_entries;
    size_t _seen = 0;
    promise<> _done;
    lw_shared_ptr<snapshots> _snapshots;
public:
    sm_value value;
    state_machine(raft::server_id id, apply_fn apply, sm_value value_, size_t apply_entries,
            lw_shared_ptr<snapshots> snapshots,
            lw_shared_ptr<persisted_snapshots> persisted_snapshots) :
        _id(id), _apply(std::move(apply)), _apply_entries(apply_entries), _snapshots(snapshots),
        value(std::move(value_)) {}
    future<> apply(const std::vector<raft::command_cref> commands) override {
        _apply(_id, commands, value);
        _seen += commands.size();
        if (_seen >= _apply_entries) {
            _done.set_value();
        }
        tlogger.debug("sm::apply[{}] got {}/{} entries", _id, _seen, _apply_entries);
        return make_ready_future<>();
    }

    future<raft::snapshot_id> take_snapshot() override {
        (*_snapshots)[_id].value = value;
        tlogger.debug("sm[{}] takes snapshot {}", _id, (*_snapshots)[_id].value.get_value());
        (*_snapshots)[_id].idx = raft::index_t{_seen};
        return make_ready_future<raft::snapshot_id>(raft::snapshot_id{utils::make_random_uuid()});
    }
    void drop_snapshot(raft::snapshot_id id) override {
        (*_snapshots).erase(_id);
    }
    future<> load_snapshot(raft::snapshot_id id) override {
        value = (*_snapshots)[_id].value;
        tlogger.debug("sm[{}] loads snapshot {}", _id, (*_snapshots)[_id].value.get_value());
        _seen = (*_snapshots)[_id].idx;
        if (_seen >= _apply_entries) {
            _done.set_value();
        }
        return make_ready_future<>();
    };
    future<> abort() override { return make_ready_future<>(); }

    future<> done() {
        return _done.get_future();
    }
};

struct Write { int32_t x; };
struct Read {};

struct Ack {};
struct Ret { int32_t x; };

namespace ser {
    template <>
    struct serializer<Read> {
        template <typename Output>
        static void write(Output& buf, const Read&) {};

        template <typename Input>
        static Read read(Input& buf) { return Read{}; }

        template <typename Input>
        static void skip(Input& buf) {}
    };

    template <>
    struct serializer<Write> {
        template <typename Output>
        static void write(Output& buf, const Write& w) { serializer<int32_t>::write(buf, w.x); };

        template <typename Input>
        static Write read(Input& buf) { return { serializer<int32_t>::read(buf) }; }

        template <typename Input>
        static void skip(Input& buf) { serializer<int32_t>::skip(buf); }
    };
}

// TODO template params etc.
using cmd_id_t = utils::UUID;
using state_t = int32_t;
using input_t = std::variant<Write, Read>;
using output_t = std::variant<Ack, Ret>;

using snapshots_t = std::unordered_map<raft::snapshot_id, state_t>;

static const state_t init_state = 0;

struct proper_state_machine : public raft::state_machine {
    raft::server_id _id;

    state_t _val;
    snapshots_t& _snapshots;

    bool _aborted = false;

    // To obtain output from an applied command, the client (`call`)
    // first allocates a channel in this data structure by calling `allocate`
    // and makes the returned command ID a part of the command passed to Raft.
    // When (if) we eventually apply the command, we use the ID to find
    // the output channel here and push the output to the client waiting
    // on the other end.
    // The client is responsible for deallocating the channels, both in the case
    // where they manage to obtain the output and when they give up.
    // The client allocates a channel only on its local server; other replicas
    // of the state machine will therefore not find the ID in their instances
    // of _output_channels so they just drop the output.
    using channels_t = std::unordered_map<cmd_id_t, promise<output_t>>;
    using channel_handle = channels_t::const_iterator;
    channels_t _output_channels;

    // perhaps this should return a future<pair<...>>? (if the transition function is a complex computation)
    // should we allow this to return some kind of errors? variant<pair<...>, error_type>?
    std::pair<output_t, state_t> transition(state_t current, input_t input) {
        using res_t = std::pair<output_t, state_t>;

        return std::visit(make_visitor(
        [] (const Write& w) -> res_t {
            return {Ack{}, w.x};
        },
        [&current] (const Read&) -> res_t {
            return {Ret{current}, std::move(current)};
        }
        ), input);
    }

    proper_state_machine(raft::server_id id, state_t init_val, snapshots_t& snapshots)
        : _id(id), _val(init_val), _snapshots(snapshots) {}

    future<> apply(std::vector<raft::command_cref> cmds) override {
        for (auto& cref : cmds) {
            auto is = ser::as_input_stream(cref);
            auto cmd_id = ser::deserialize(is, boost::type<cmd_id_t>{});
            auto input = ser::deserialize(is, boost::type<input_t>{});
            auto [output, new_state] = transition(std::move(_val), std::move(input));
            _val = std::move(new_state);

            auto it = _output_channels.find(cmd_id);
            if (it != _output_channels.end()) {
                // We are on the leader server where the client submitted the command
                // and waits for the output. Send it to them.
                it->second.set_value(std::move(output));
            } else {
                // This is not the leader on which the command was submitted,
                // or it is but the client already gave up on us and deallocated the channel.
                // In any case we simply drop the output.
            }

            if (_aborted) {
                co_return;
            }

            co_await make_ready_future<>(); // maybe yield
        }
    }

    future<raft::snapshot_id> take_snapshot() override {
        auto id = raft::snapshot_id{utils::make_random_uuid()};
        _snapshots[id] = _val;
        co_return id;
    }

    void drop_snapshot(raft::snapshot_id id) override {
        _snapshots.erase(id);
    }

    future<> load_snapshot(raft::snapshot_id id) override {
        auto it = _snapshots.find(id);
        assert(it != _snapshots.end()); // dunno if the snapshot can actually be missing
        _val = it->second;
        co_return;
    }

    future<> abort() override {
        _aborted = true;
        // TODO: the semantics of abort are not clear.
        // If apply is in progress, should we wait for it to finish here? (it will finish on the next iteration over cmds)
        co_return;
    }

    // TODO document
    // the promise is valid until and only until the channel is deallocated
    // using `deallocate`. The caller must not attempt to wait for the output after
    // deallocating the channel.
    std::pair<cmd_id_t, channel_handle> allocate(promise<output_t> p) {
        auto cmd_id = utils::make_random_uuid();
        auto [it, inserted] = _output_channels.emplace(cmd_id, std::move(p));
        assert(inserted);
        return {cmd_id, it};
    }

    void deallocate(channel_handle h) noexcept {
        _output_channels.erase(h);
    }
};

raft::command make_command(const cmd_id_t& cmd_id, const input_t& input) {
    raft::command cmd;
    ser::serialize(cmd, cmd_id);
    ser::serialize(cmd, input);
    return cmd;
}

// TODO: template params etc.
using call_error_t = std::variant<timed_out_error, raft::not_a_leader>; // TODO
using result_t = std::variant<output_t, call_error_t>;
future<result_t> call(input_t input, raft::clock_type::time_point timeout, raft::server& server, proper_state_machine& sm) {
    auto p = promise<output_t>{};
    auto f = p.get_future();
    auto [cmd_id, channel_handle] = sm.allocate(std::move(p)); // TODO: deallocate on failure (finally?)

    try {
        co_await server.add_entry(
                make_command(std::move(cmd_id), std::move(input)),
                raft::wait_type::committed);
    } catch (raft::not_a_leader ex) {
        sm.deallocate(std::move(channel_handle));
        co_return ex;
    }

    try {
        auto output = co_await with_timeout(timeout, std::move(f));
        sm.deallocate(std::move(channel_handle));
        co_return output;
    } catch (timed_out_error ex) {
        sm.deallocate(std::move(channel_handle));
        co_return ex;
    }
}

template <typename Payload>
struct network {
    struct message {
        raft::server_id src;
        raft::server_id dst;

        // shared ptr to implement duplication of messages
        lw_shared_ptr<Payload> payload;
    };

    struct event {
        raft::logical_clock::time_point time;
        message msg;
    };

    // A min-heap of event occurences compared by their time points.
    std::vector<event> _events;
    static bool cmp(const event& o1, const event& o2) {
        return o1.time > o2.time;
    }

    raft::logical_clock _clock;

    using deliver_t = std::function<void(raft::server_id src, raft::server_id dst, const Payload&)>;
    deliver_t _deliver;

    // A pair (dst, [src1, src2, ...]) in this set denotes that `dst`
    // does not receive messages from src1, src2, ...
    std::unordered_map<raft::server_id, std::unordered_set<raft::server_id>> _grudges;

    network(deliver_t f)
        : _deliver(std::move(f)) {}

    void send(raft::server_id src, raft::server_id dst, const Payload& payload) {
        // Predict the delivery time in advance.
        // Our prediction may be wrong if a grudge exists at this expected moment of delivery.
        // todo: scale with number of msgs already in transit and payload size?
        // todo: randomize
        auto delivery_time = _clock.now() + 20ms;

        _events.push_back(event{delivery_time, message{src, dst, make_lw_shared<Payload>(payload)}});
        std::push_heap(_events.begin(), _events.end(), cmp);
    }

    void deliver() {
        while (!_events.empty() && _events.front().time > _clock.now()) {
            std::pop_heap(_events.begin(), _events.end(), cmp);
            auto [_, m] = std::move(_events.back());
            _events.pop_back();

            if (!_grudges[m.dst].contains(m.src)) {
                _deliver(m.src, m.dst, std::move(m.payload));
            }
        }
    }

    void advance(raft::logical_clock::duration d) {
        _clock.advance(d);
        deliver();
    }
};

struct network_interface : public raft::rpc, public raft::failure_detector {
    using reply_id_t = uint32_t;

    struct snapshot_message {
        raft::install_snapshot ins;
        state_t snapshot_payload; // TODO: maybe serialized would be better? or use some type erasure? otherwise we need to template
        reply_id_t reply_id;
    };

    struct snapshot_reply_message {
        raft::snapshot_reply reply;
        reply_id_t reply_id;
    };

    struct heartbeat {};

    using message_t = std::variant<
        snapshot_message,
        snapshot_reply_message,
        raft::append_request,
        raft::append_reply,
        raft::vote_request,
        raft::vote_reply,
        raft::timeout_now,
        heartbeat>;

    raft::server_id _id;
    network<message_t>& _net;
    snapshots_t& _snapshots;

    raft::logical_clock _clock;

    // The set of known servers, used to broadcast heartbeats.
    // The second element of the pair denotes an expiration time after which we remove the server from this set.
    // Most servers won't have an expiration time; it is assigned only to servers from which we receive a message
    // without having them added locally with `add_server`.
    std::unordered_map<raft::server_id, std::optional<raft::logical_clock::time_point>> _known;

    raft::logical_clock::time_point _last_beat;

    // The last time we received a heartbeat from a server.
    std::unordered_map<raft::server_id, raft::logical_clock::time_point> _last_heard;

    std::unordered_map<reply_id_t, promise<raft::snapshot_reply>> _reply_promises;
    reply_id_t _counter = 0;

    network_interface(raft::server_id id, network<message_t>& net, snapshots_t& snaps)
        : _id(id), _net(net), _snapshots(snaps)
    {}

    // Message is delivered to us
    future<> receive(raft::server_id src, message_t payload) { // TODO const message_t&? and everywhere else
        assert(_client);
        auto& c = *_client;

        {
            // todo: make adjustable
            static const raft::logical_clock::duration _known_gc_threshold = 30s;
            auto it = _known.find(src);
            if (it == _known.end() || it->second) {
                _known.emplace(src, _clock.now() + _known_gc_threshold);
            }
        }

        co_await std::visit(make_visitor(
        [&] (snapshot_message m) -> future<> {
            _snapshots.emplace(m.ins.snp.id, std::move(m.snapshot_payload));
            auto reply = co_await c.apply_snapshot(src, std::move(m.ins));

            _net.send(_id, src, snapshot_reply_message{
                .reply = std::move(reply),
                .reply_id = m.reply_id
            });
        },
        [this] (snapshot_reply_message m) -> future<> {
            auto it = _reply_promises.find(m.reply_id);
            if (it != _reply_promises.end()) {
                it->second.set_value(std::move(m.reply));
            }
            co_return;
        },
        [&] (raft::append_request m) -> future<> {
            c.append_entries(src, std::move(m));
            co_return;
        },
        [&] (raft::append_reply m) -> future<> {
            c.append_entries_reply(src, std::move(m));
            co_return;
        },
        [&] (raft::vote_request m) -> future<> {
            c.request_vote(src, std::move(m));
            co_return;
        },
        [&] (raft::vote_reply m) -> future<> {
            c.request_vote_reply(src, std::move(m));
            co_return;
        },
        [&] (raft::timeout_now m) -> future<> {
            c.timeout_now_request(src, std::move(m));
            co_return;
        },
        [&] (heartbeat) -> future<> {
            _last_heard[src] = _clock.now();
            co_return;
        }
        ), std::move(payload));
    }

    virtual future<raft::snapshot_reply> send_snapshot(raft::server_id dst, const raft::install_snapshot& ins) override {
        auto it = _snapshots.find(ins.snp.id);
        assert(it != _snapshots.end());

        auto id = _counter++;
        auto f = _reply_promises[id].get_future();

        _net.send(_id, dst, snapshot_message{
            .ins = ins,
            .snapshot_payload = it->second,
            .reply_id = id
        });

        auto reply = co_await std::move(f); // TODO timeout, abort
        _reply_promises.erase(id); // TODO exception safety, finally

        co_return reply;
    }

    virtual future<> send_append_entries(raft::server_id dst, const raft::append_request& m) override {
        _net.send(_id, dst, m);
        co_return;
    }

    virtual future<> send_append_entries_reply(raft::server_id dst, const raft::append_reply& m) override {
        _net.send(_id, dst, m);
        co_return;
    }

    virtual future<> send_vote_request(raft::server_id dst, const raft::vote_request& m) override {
        _net.send(_id, dst, m);
        co_return;
    }

    virtual future<> send_vote_reply(raft::server_id dst, const raft::vote_reply& m) override {
        _net.send(_id, dst, m);
        co_return;
    }

    virtual future<> send_timeout_now(raft::server_id dst, const raft::timeout_now& m) override {
        _net.send(_id, dst, m);
        co_return;
    }

    virtual void add_server(raft::server_id id, raft::server_info) override {
        _known.emplace(id, std::nullopt);
    }

    virtual void remove_server(raft::server_id id) override {
        _known.erase(id);
    }

    virtual future<> abort() override {
        // TODO
        co_return;
    }

    bool is_alive(raft::server_id id) override {
        // todo: make it adjustable
        static const raft::logical_clock::duration _convict_threshold = 1s;

        return _clock.now() - _last_heard[id] <= _convict_threshold;
    }

    void advance(raft::logical_clock::duration d) {
        _clock.advance(d);

        // todo: make it adjustable
        static const raft::logical_clock::duration _heartbeat_period = 100ms;
        if (_clock.now() - _last_beat > _heartbeat_period) {
            for (auto& [dst, _] : _known) {
                _net.send(_id, dst, heartbeat{});
            }
            _last_beat = _clock.now();
        }

        // Garbage-collect _known servers
        std::vector<raft::server_id> to_erase;
        for (auto& [dst, gc_time_opt] : _known) {
            if (gc_time_opt && _clock.now() > *gc_time_opt) {
                to_erase.push_back(dst);
            }
        }

        for (auto dst : to_erase) {
            _known.erase(dst);
        }
    }
};

// TODO: introduce yields and delays; use logical clock steered from outside as well?
// template state_t
struct proper_persistence : public raft::persistence {
    snapshots_t& _snapshots;

    std::optional<std::pair<raft::snapshot, state_t>> _stored_snapshot;
    std::optional<std::pair<raft::term_t, raft::server_id>> _stored_term_and_vote;


    // Invariant: for each entry except the first, the raft index is equal to the raft index of the previous entry plus one.
    raft::log_entries _stored_entries;

    // Instances of `store_log_entries` waiting for holes to be filled before they store their entries;
    // an (idx, p) entry in this map denotes that there is a waiter which wants the log to be filled
    // up to raft index (but not necessarily including) `idx`, and is waiting on the other side of `p`.
    // The waiters do not timeout. It is the responsibility of the one who resolves the future
    // to clear it from the map.
    std::unordered_multimap<uint64_t, promise<>> _waiters;

    proper_persistence(snapshots_t& snaps)
        : _snapshots(snaps)
    {}

    // Returns an iterator to the entry in `_stored_entries` whose raft index is `idx` if the entry exists.
    // If all entries in `_stored_entries` have greater indexes, return the first one.
    // If all entries have smaller indexes, return end().
    raft::log_entries::iterator find(raft::index_t idx) {
        // The correctness of this depends on the `_stored_entries` invariant.
        auto b = _stored_entries.begin();
        if (b == _stored_entries.end() || (*b)->idx >= idx) {
            return b;
        }
        return b + std::min((idx - (*b)->idx).get_value(), _stored_entries.size());
    }

    virtual future<> store_term_and_vote(raft::term_t term, raft::server_id vote) override {
        _stored_term_and_vote = std::pair{term, vote};
        co_return;
    }

    virtual future<std::pair<raft::term_t, raft::server_id>> load_term_and_vote() override {
        // TODO: actually this can be nullopt; what to do? should the constructor get some initial term and vote?
        // that wouldn't make sense tho, we don't know who to vote for when we start (or do we?)
        assert(_stored_term_and_vote);
        co_return *_stored_term_and_vote;
    }

    virtual future<> store_snapshot(const raft::snapshot& snap, size_t preserve_log_entries) override {
        auto it = _snapshots.find(snap.id);
        assert(it != _snapshots.end());
        _stored_snapshot.emplace(snap, it->second);

        auto first_to_remain = snap.idx + 1 >= preserve_log_entries ? raft::index_t{snap.idx + 1 - preserve_log_entries} : raft::index_t{0};
        _stored_entries.erase(_stored_entries.begin(), find(first_to_remain));

        co_return;
    }

    virtual future<raft::snapshot> load_snapshot() override {
        assert(_stored_snapshot);
        auto [snap, state] = *_stored_snapshot;
        _snapshots[snap.id] = std::move(state);
        co_return snap;
    }

    virtual future<> store_log_entries(const std::vector<raft::log_entry_ptr>& entries) {
        // TODO: we assert that `entries` is contiguous w.r.t entry indexes;
        // the interface comment doesn't say it, but it wouldn't make much sense otherwise
        for (size_t i = 1; i < entries.size(); ++i) {
            assert(entries[i]->idx == entries[i-1]->idx);
        }

        if (entries.empty()) {
            co_return;
        }

        // If the first stored entry has a greater index than `entries.back()`, then storing
        // `entries` would create a hole in the log which we don't allow (see invariant).
        // We argue that this situation should not be possible and implies a bug. Indeed:
        // let E be the first currently stored entry (`*_stored_entries.begin()`) and
        // let E' = `entries.back()`. Suppose that E'.idx < E.idx; in particular, the log
        // does not currently contain an entry with raft index E'.idx. Thus we have either
        // started with a snapshot S with S.idx > E'.idx, or stored a snapshot S with S.idx > E'.idx
        // which caused the entry at E'.idx to be dropped. In either case there is no reason
        // to store log entries with indices >= S.idx, in particular those in `entries`.
        // TODO: perhaps some weird race may cause this; if that's true, then we can probably
        // ignore those entries since we have a later snapshot anyway. Leaving the check
        // in attempt to catch that race (if it's there) so we can think whether it makes sense.
        assert(_stored_entries.empty() || entries.back()->idx >= _stored_entries.front()->idx);

        auto first_idx = entries.front()->idx;
        auto first_it = find(first_idx);

        if (first_it == _stored_entries.end() && !_stored_entries.empty() && first_idx > _stored_entries.back()->idx + 1) {
            // There is a gap between the last entry in the log and first_idx.
            // We must wait until it becomes filled.
            promise<> p;
            auto f = p.get_future();
            _waiters.emplace(first_idx.get_value(), std::move(p));
            co_await std::move(f);

            // The future is resolved so the gap was filled.
            // However, a snapshot might have been stored in the meantime, causing some entries to be removed.
            // If the indexes of those removed entries were all <= than the index of the last entry in `entries`
            // then storing `entries` is safe (it can't cause a hole to appear). But if they had greater indexes,
            // then storing `entries` now would cause a hole to appear (to the right side of `entries`) giving
            // the same situation as described above. TODO: as above, not sure if we should allow it (and simply ignore `entries`).
            assert(_stored_entries.empty() || entries.back()->idx >= _stored_entries.front()->idx);

            first_it = find(first_idx);
        }

        assert(_stored_entries.empty()
                || (first_it == _stored_entries.end() && _stored_entries.back()->idx + 1 == first_idx)
                || (first_it != _stored_entries.end() && (*first_it)->idx == first_idx));

        // Indexes from `entries` may intersect existing indexes.
        // Replace the intersection and push back the rest.
        auto end_it = find(entries.back()->idx);
        assert(end_it - first_it <= entries.size());

        if (end_it != _stored_entries.end()) {
            assert((*end_it)->idx == entries.back()->idx);
            // We will replace the range [first_it, end_it).
            ++end_it;
        }

        // Replace the intersection
        auto it = entries.begin();
        while (first_it != end_it) {
            *(first_it++) = *(it++);
        }

        // Push back the rest
        while (it != entries.end()) {
            _stored_entries.push_back(*it++);
        }

        // TODO inform waiters

        co_return;
    }

    virtual future<raft::log_entries> load_log() {
        co_return _stored_entries;
    }

    virtual future<> truncate_log(raft::index_t idx) {
        _stored_entries.erase(find(idx), _stored_entries.end());
        co_return;
    }

    virtual future<> abort() {
        // TODO
        co_return;
    }
};

struct initial_state {
    raft::server_address address;
    raft::term_t term = raft::term_t(1);
    raft::server_id vote;
    std::vector<raft::log_entry> log;
    raft::snapshot snapshot;
    snapshot_value snp_value;
    raft::server::configuration server_config = raft::server::configuration{.append_request_threshold = 200};
};

class persistence : public raft::persistence {
    raft::server_id _id;
    initial_state _conf;
    lw_shared_ptr<snapshots> _snapshots;
    lw_shared_ptr<persisted_snapshots> _persisted_snapshots;
public:
    persistence(raft::server_id id, initial_state conf, lw_shared_ptr<snapshots> snapshots,
            lw_shared_ptr<persisted_snapshots> persisted_snapshots) : _id(id),
            _conf(std::move(conf)), _snapshots(snapshots),
            _persisted_snapshots(persisted_snapshots) {}
    persistence() {}
    virtual future<> store_term_and_vote(raft::term_t term, raft::server_id vote) { return seastar::sleep(1us); }
    virtual future<std::pair<raft::term_t, raft::server_id>> load_term_and_vote() {
        auto term_and_vote = std::make_pair(_conf.term, _conf.vote);
        return make_ready_future<std::pair<raft::term_t, raft::server_id>>(term_and_vote);
    }
    virtual future<> store_snapshot(const raft::snapshot& snap, size_t preserve_log_entries) {
        (*_persisted_snapshots)[_id] = std::make_pair(snap, (*_snapshots)[_id]);
        tlogger.debug("sm[{}] persists snapshot {}", _id, (*_snapshots)[_id].value.get_value());
        return make_ready_future<>();
    }
    future<raft::snapshot> load_snapshot() override {
        return make_ready_future<raft::snapshot>(_conf.snapshot);
    }
    virtual future<> store_log_entries(const std::vector<raft::log_entry_ptr>& entries) { return seastar::sleep(1us); };
    virtual future<raft::log_entries> load_log() {
        raft::log_entries log;
        for (auto&& e : _conf.log) {
            log.emplace_back(make_lw_shared(std::move(e)));
        }
        return make_ready_future<raft::log_entries>(std::move(log));
    }
    virtual future<> truncate_log(raft::index_t idx) { return make_ready_future<>(); }
    virtual future<> abort() { return make_ready_future<>(); }
};

struct connected {
    // Usually a test wants to disconnect a leader or very few nodes
    // so it makes sense to just track those
    lw_shared_ptr<std::unordered_set<raft::server_id>> _disconnected;
    // Default copy constructor for other users
    connected() {
        _disconnected = make_lw_shared<std::unordered_set<raft::server_id>>();
    }
    void disconnect(raft::server_id id) {
        _disconnected->insert(id);
    }
    void connect(raft::server_id id) {
        _disconnected->erase(id);
    }
    void connect_all() {
        _disconnected->clear();
    }
    bool operator()(raft::server_id id) {
        return _disconnected->find(id) == _disconnected->end();
    }
};

class failure_detector : public raft::failure_detector {
    raft::server_id _id;
    connected _connected;
public:
    failure_detector(raft::server_id id, connected connected) : _id(id), _connected(connected) {}
    bool is_alive(raft::server_id server) override {
        return _connected(server) && _connected(_id);
    }
};

class rpc : public raft::rpc {
    static std::unordered_map<raft::server_id, rpc*> net;
    raft::server_id _id;
    connected _connected;
    lw_shared_ptr<snapshots> _snapshots;
    bool _packet_drops;
public:
    rpc(raft::server_id id, connected connected, lw_shared_ptr<snapshots> snapshots,
            bool packet_drops) : _id(id), _connected(connected), _snapshots(snapshots),
            _packet_drops(packet_drops) {
        net[_id] = this;
    }
    virtual future<raft::snapshot_reply> send_snapshot(raft::server_id id, const raft::install_snapshot& snap) {
        if (!_connected(id) || !_connected(_id)) {
            return make_ready_future<raft::snapshot_reply>(raft::snapshot_reply{
                    .current_term = snap.current_term,
                    .success = false});
        }
        (*_snapshots)[id] = (*_snapshots)[_id];
        return net[id]->_client->apply_snapshot(_id, std::move(snap));
    }
    virtual future<> send_append_entries(raft::server_id id, const raft::append_request& append_request) {
        if (!_connected(id) || !_connected(_id) || (_packet_drops && !(rand() % 5))) {
            return make_ready_future<>();
        }
        net[id]->_client->append_entries(_id, append_request);
        return make_ready_future<>();
    }
    virtual future<> send_append_entries_reply(raft::server_id id, const raft::append_reply& reply) {
        if (!_connected(id) || !_connected(_id) || (_packet_drops && !(rand() % 5))) {
            return make_ready_future<>();
        }
        net[id]->_client->append_entries_reply(_id, std::move(reply));
        return make_ready_future<>();
    }
    virtual future<> send_vote_request(raft::server_id id, const raft::vote_request& vote_request) {
        if (!_connected(id) || !_connected(_id)) {
            return make_ready_future<>();
        }
        net[id]->_client->request_vote(_id, std::move(vote_request));
        return make_ready_future<>();
    }
    virtual future<> send_vote_reply(raft::server_id id, const raft::vote_reply& vote_reply) {
        if (!_connected(id) || !_connected(_id)) {
            return make_ready_future<>();
        }
        net[id]->_client->request_vote_reply(_id, std::move(vote_reply));
        return make_ready_future<>();
    }
    virtual future<> send_timeout_now(raft::server_id id, const raft::timeout_now& timeout_now) {
        if (!_connected(id) || !_connected(_id)) {
            return make_ready_future<>();
        }
        net[id]->_client->timeout_now_request(_id, std::move(timeout_now));
        return make_ready_future<>();
    }
    virtual void add_server(raft::server_id id, bytes node_info) {}
    virtual void remove_server(raft::server_id id) {}
    virtual future<> abort() { return make_ready_future<>(); }
};

std::unordered_map<raft::server_id, rpc*> rpc::net;

enum class sm_type {
    HASH,
    SUM
};

std::pair<std::unique_ptr<raft::server>, state_machine*>
create_raft_server(raft::server_id uuid, state_machine::apply_fn apply, initial_state state,
        size_t apply_entries, sm_type type, connected connected, lw_shared_ptr<snapshots> snapshots,
        lw_shared_ptr<persisted_snapshots> persisted_snapshots, bool packet_drops) {
    sm_value val = (type == sm_type::HASH) ? sm_value(std::make_unique<hasher_int>()) : sm_value(std::make_unique<sum_sm>());

    auto sm = std::make_unique<state_machine>(uuid, std::move(apply), std::move(val),
            apply_entries, snapshots, persisted_snapshots);
    auto& rsm = *sm;
    auto mrpc = std::make_unique<rpc>(uuid, connected, snapshots, packet_drops);
    auto mpersistence = std::make_unique<persistence>(uuid, state, snapshots, persisted_snapshots);
    auto fd = seastar::make_shared<failure_detector>(uuid, connected);

    auto raft = raft::create_server(uuid, std::move(mrpc), std::move(sm), std::move(mpersistence),
        std::move(fd), state.server_config);

    return std::make_pair(std::move(raft), &rsm);
}

future<std::vector<std::pair<std::unique_ptr<raft::server>, state_machine*>>> create_cluster(std::vector<initial_state> states, state_machine::apply_fn apply, size_t apply_entries, sm_type type,
        connected connected, lw_shared_ptr<snapshots> snapshots,
        lw_shared_ptr<persisted_snapshots> persisted_snapshots, bool packet_drops) {
    raft::configuration config;
    std::vector<std::pair<std::unique_ptr<raft::server>, state_machine*>> rafts;

    for (size_t i = 0; i < states.size(); i++) {
        states[i].address = raft::server_address{to_raft_id(i)};
        config.current.emplace(states[i].address);
    }

    for (size_t i = 0; i < states.size(); i++) {
        auto& s = states[i].address;
        states[i].snapshot.config = config;
        (*snapshots)[s.id] = states[i].snp_value;
        auto& raft = *rafts.emplace_back(create_raft_server(s.id, apply, states[i], apply_entries,
                    type, connected, snapshots, persisted_snapshots, packet_drops)).first;
        co_await raft.start();
    }

    co_return std::move(rafts);
}

struct log_entry {
    unsigned term;
    int value;
};

std::vector<raft::log_entry> create_log(std::vector<log_entry> list, unsigned start_idx) {
    std::vector<raft::log_entry> log;

    unsigned i = start_idx;
    for (auto e : list) {
        raft::command command;
        ser::serialize(command, e.value);
        log.push_back(raft::log_entry{raft::term_t(e.term), raft::index_t(i++), std::move(command)});
    }

    return log;
}

template <typename T>
std::vector<raft::command> create_commands(std::vector<T> list) {
    std::vector<raft::command> commands;
    commands.reserve(list.size());

    for (auto e : list) {
        raft::command command;
        ser::serialize(command, e);
        commands.push_back(std::move(command));
    }

    return commands;
}

void apply_changes(raft::server_id id, const std::vector<raft::command_cref>& commands, sm_value& value) {
    tlogger.debug("sm::apply_changes[{}] got {} entries", id, commands.size());

    for (auto&& d : commands) {
        auto is = ser::as_input_stream(d);
        int n = ser::deserialize(is, boost::type<int>());
        value.update(n);      // running hash (values and snapshots)
        tlogger.debug("{}: apply_changes {}", id, n);
    }
};

// Updates can be
//  - Entries
//  - Leader change
//  - Configuration change
using entries = unsigned;
using new_leader = int;
struct leader {
    size_t id;
};
using partition = std::vector<std::variant<leader,int>>;
// TODO: config change
using update = std::variant<entries, new_leader, partition>;

struct initial_log {
    std::vector<log_entry> le;
};

struct initial_snapshot {
    raft::snapshot snap;
};

struct test_case {
    const sm_type type = sm_type::HASH;
    const size_t nodes;
    const size_t total_values = 100;
    uint64_t initial_term = 1;
    const std::optional<uint64_t> initial_leader;
    const std::vector<struct initial_log> initial_states;
    const std::vector<struct initial_snapshot> initial_snapshots;
    const std::vector<raft::server::configuration> config;
    const std::vector<update> updates;
};

future<> wait_log(std::vector<std::pair<std::unique_ptr<raft::server>, state_machine*>>& rafts,
        connected& connected, size_t leader) {
    // Wait for leader log to propagate
    auto leader_log_idx = rafts[leader].first->log_last_idx();
    for (size_t s = 0; s < rafts.size(); ++s) {
        if (s != leader && connected(to_raft_id(s))) {
            co_await rafts[s].first->wait_log_idx(leader_log_idx);
        }
    }
}

void pause_tickers(std::vector<seastar::timer<lowres_clock>>& tickers) {
    for (auto& ticker: tickers) {
        ticker.cancel();
    }
}

void restart_tickers(std::vector<seastar::timer<lowres_clock>>& tickers) {
    for (auto& ticker: tickers) {
        ticker.rearm_periodic(tick_delta);
    }
}

// Run test case (name, nodes, leader, initial logs, updates)
future<> run_test(test_case test, bool packet_drops) {
    std::vector<initial_state> states(test.nodes);       // Server initial states

    size_t leader;
    if (test.initial_leader) {
        leader = *test.initial_leader;
    } else {
        leader = 0;
    }

    states[leader].term = raft::term_t{test.initial_term};

    int leader_initial_entries = 0;
    if (leader < test.initial_states.size()) {
        leader_initial_entries += test.initial_states[leader].le.size();  // Count existing leader entries
    }
    int leader_snap_skipped = 0; // Next value to write
    if (leader < test.initial_snapshots.size()) {
        leader_snap_skipped = test.initial_snapshots[leader].snap.idx;  // Count existing leader entries
    }

    auto sm_value_for = [&] (int max) {
        return test.type == sm_type::HASH ? hasher_int::value_for(max) : sum_sm::value_for(max);
    };

    // Server initial logs, etc
    for (size_t i = 0; i < states.size(); ++i) {
        size_t start_idx = 1;
        if (i < test.initial_snapshots.size()) {
            states[i].snapshot = test.initial_snapshots[i].snap;
            states[i].snp_value.value = sm_value_for(test.initial_snapshots[i].snap.idx);
            states[i].snp_value.idx = test.initial_snapshots[i].snap.idx;
            start_idx = states[i].snapshot.idx + 1;
        }
        if (i < test.initial_states.size()) {
            auto state = test.initial_states[i];
            states[i].log = create_log(state.le, start_idx);
        } else {
            states[i].log = {};
        }
        if (i < test.config.size()) {
            states[i].server_config = test.config[i];
        }
    }

    auto snaps = make_lw_shared<snapshots>();
    auto persisted_snaps = make_lw_shared<persisted_snapshots>();
    connected connected{};

    auto rafts = co_await create_cluster(states, apply_changes, test.total_values, test.type,
            connected, snaps, persisted_snaps, packet_drops);

    // Tickers for servers
    std::vector<seastar::timer<lowres_clock>> tickers(test.nodes);
    for (size_t s = 0; s < test.nodes; ++s) {
        tickers[s].arm_periodic(tick_delta);
        tickers[s].set_callback([&rafts, s] {
            rafts[s].first->tick();
        });
    }

    BOOST_TEST_MESSAGE("Electing first leader " << leader);
    co_await rafts[leader].first->elect_me_leader();
    BOOST_TEST_MESSAGE("Processing updates");
    // Process all updates in order
    size_t next_val = leader_snap_skipped + leader_initial_entries;
    for (auto update: test.updates) {
        if (std::holds_alternative<entries>(update)) {
            auto n = std::get<entries>(update);
            std::vector<int> values(n);
            std::iota(values.begin(), values.end(), next_val);
            std::vector<raft::command> commands = create_commands<int>(values);
            co_await seastar::do_for_each(commands, [&] (raft::command cmd) {
                tlogger.debug("Adding command entry on leader {}", leader);
                return rafts[leader].first->add_entry(std::move(cmd), raft::wait_type::committed);
            });
            next_val += n;
            co_await wait_log(rafts, connected, leader);
        } else if (std::holds_alternative<new_leader>(update)) {
            co_await wait_log(rafts, connected, leader);
            pause_tickers(tickers);
            unsigned next_leader = std::get<new_leader>(update);
            if (next_leader != leader) {
                BOOST_CHECK_MESSAGE(next_leader < rafts.size(),
                        format("Wrong next leader value {}", next_leader));
                // Wait for leader log to propagate
                auto leader_log_idx = rafts[leader].first->log_last_idx();
                for (size_t s = 0; s < test.nodes; ++s) {
                    if (s != leader && connected(to_raft_id(s))) {
                        co_await rafts[s].first->wait_log_idx(leader_log_idx);
                    }
                }
                // Make current leader a follower: disconnect, timeout, re-connect
                connected.disconnect(to_raft_id(leader));
                for (size_t s = 0; s < test.nodes; ++s) {
                    rafts[s].first->elapse_election();
                }
                co_await rafts[next_leader].first->elect_me_leader();
                connected.connect(to_raft_id(leader));
                tlogger.debug("confirmed leader on {}", next_leader);
                leader = next_leader;
            }
            restart_tickers(tickers);
        } else if (std::holds_alternative<partition>(update)) {
            co_await wait_log(rafts, connected, leader);
            pause_tickers(tickers);
            auto p = std::get<partition>(update);
            connected.connect_all();
            std::unordered_set<size_t> partition_servers;
            struct leader new_leader;
            bool have_new_leader = false;
            for (auto s: p) {
                size_t id;
                if (std::holds_alternative<struct leader>(s)) {
                    have_new_leader = true;
                    new_leader = std::get<struct leader>(s);
                    id = new_leader.id;
                } else {
                    id = std::get<int>(s);
                }
                partition_servers.insert(id);
            }
            for (size_t s = 0; s < test.nodes; ++s) {
                if (partition_servers.find(s) == partition_servers.end()) {
                    // Disconnect servers not in main partition
                    connected.disconnect(to_raft_id(s));
                }
            }
            if (have_new_leader && new_leader.id != leader) {
                // New leader specified, elect it
                for (size_t s = 0; s < test.nodes; ++s) {
                    rafts[s].first->elapse_election();
                }
                co_await rafts[new_leader.id].first->elect_me_leader();
                tlogger.debug("confirmed leader on {}", new_leader.id);
                leader = new_leader.id;
            } else if (partition_servers.find(leader) == partition_servers.end() && p.size() > 0) {
                // Old leader disconnected and not specified new, free election
                for (size_t s = 0; s < test.nodes; ++s) {
                    rafts[s].first->elapse_election();
                }
                for (bool have_leader = false; !have_leader; ) {
                    for (auto s: partition_servers) {
                        rafts[s].first->tick();
                    }
                    co_await later();                 // yield
                    for (auto s: partition_servers) {
                        if (rafts[s].first->is_leader()) {
                            have_leader = true;
                            leader = s;
                            break;
                        }
                    }
                }
                tlogger.debug("confirmed new leader on {}", leader);
            }
            restart_tickers(tickers);
        }
    }

    connected.connect_all();

    BOOST_TEST_MESSAGE("Appending remaining values");
    if (next_val < test.total_values) {
        // Send remaining updates
        std::vector<int> values(test.total_values - next_val);
        std::iota(values.begin(), values.end(), next_val);
        std::vector<raft::command> commands = create_commands<int>(values);
        tlogger.debug("Adding remaining {} entries on leader {}", values.size(), leader);
        co_await seastar::do_for_each(commands, [&] (raft::command cmd) {
            return rafts[leader].first->add_entry(std::move(cmd), raft::wait_type::committed);
        });
    }

    // Wait for all state_machine s to finish processing commands
    for (auto& r:  rafts) {
        co_await r.second->done();
    }

    for (auto& r: rafts) {
        co_await r.first->abort(); // Stop servers
    }

    BOOST_TEST_MESSAGE("Verifying hashes match expected (snapshot and apply calls)");
    auto expected = sm_value_for(test.total_values).get_value();
    for (size_t i = 0; i < rafts.size(); ++i) {
        auto digest = rafts[i].second->value.get_value();
        BOOST_CHECK_MESSAGE(digest == expected,
                format("Digest doesn't match for server [{}]: {} != {}", i, digest, expected));
    }

    BOOST_TEST_MESSAGE("Verifying persisted snapshots");
    // TODO: check that snapshot is taken when it should be
    for (auto& s : (*persisted_snaps)) {
        auto& [snp, val] = s.second;
        auto& digest = val.value;
        auto expected = sm_value_for(val.idx);
        BOOST_CHECK_MESSAGE(digest == expected,
                format("Persisted snapshot {} doesn't match {} != {}", snp.id, digest.get_value(), expected.get_value()));
   }

    co_return;
}

void replication_test(struct test_case test, bool packet_drops) {
    run_test(std::move(test), packet_drops).get();
}

#define RAFT_TEST_CASE(test_name, test_body)  \
    SEASTAR_THREAD_TEST_CASE(test_name) { replication_test(test_body, false); }  \
    SEASTAR_THREAD_TEST_CASE(test_name ## _drops) { replication_test(test_body, true); }

// 1 nodes, simple replication, empty, no updates
RAFT_TEST_CASE(simple_replication, (test_case{
         .nodes = 1}))

// 2 nodes, 4 existing leader entries, 4 updates
RAFT_TEST_CASE(non_empty_leader_log, (test_case{
         .nodes = 2,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3}}}},
         .updates = {entries{4}}}));

// 2 nodes, don't add more entries besides existing log
RAFT_TEST_CASE(non_empty_leader_log_no_new_entries, (test_case{
         .nodes = 2, .total_values = 4,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3}}}}}));

// 1 nodes, 12 client entries
RAFT_TEST_CASE(simple_1_auto_12, (test_case{
         .nodes = 1,
         .initial_states = {}, .updates = {entries{12}}}));

// 1 nodes, 12 client entries
RAFT_TEST_CASE(simple_1_expected, (test_case{
         .nodes = 1, .initial_states = {},
         .updates = {entries{4}}}));

// 1 nodes, 7 leader entries, 12 client entries
RAFT_TEST_CASE(simple_1_pre, (test_case{
         .nodes = 1,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}}},
         .updates = {entries{12}},}));

// 2 nodes, 7 leader entries, 12 client entries
RAFT_TEST_CASE(simple_2_pre, (test_case{
         .nodes = 2,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}}},
         .updates = {entries{12}},}));

// 3 nodes, 2 leader changes with 4 client entries each
RAFT_TEST_CASE(leader_changes, (test_case{
         .nodes = 3,
         .updates = {entries{4},new_leader{1},entries{4},new_leader{2},entries{4}}}));

//
// NOTE: due to disrupting candidates protection leader doesn't vote for others, and
//       servers with entries vote for themselves, so some tests use 3 servers instead of
//       2 for simplicity and to avoid a stalemate. This behaviour can be disabled.
//

// 3 nodes, 7 leader entries, 12 client entries, change leader, 12 client entries
RAFT_TEST_CASE(simple_3_pre_chg, (test_case{
         .nodes = 3, .initial_term = 2,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}}},
         .updates = {entries{12},new_leader{1},entries{12}},}));

// 2 nodes, leader empoty, follower has 3 spurious entries
RAFT_TEST_CASE(replace_log_leaders_log_empty, (test_case{
         .nodes = 3, .initial_term = 2,
         .initial_states = {{}, {{{2,10},{2,20},{2,30}}}},
         .updates = {entries{4}}}));

// 3 nodes, 7 leader entries, follower has 9 spurious entries
RAFT_TEST_CASE(simple_3_spurious_1, (test_case{
         .nodes = 3, .initial_term = 2,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}},
                            {{{2,10},{2,11},{2,12},{2,13},{2,14},{2,15},{2,16},{2,17},{2,18}}}},
         .updates = {entries{4}},}));

// 3 nodes, term 3, leader has 9 entries, follower has 5 spurious entries, 4 client entries
RAFT_TEST_CASE(simple_3_spurious_2, (test_case{
         .nodes = 3, .initial_term = 3,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}},
                            {{{2,10},{2,11},{2,12},{2,13},{2,14}}}},
         .updates = {entries{4}},}));

// 3 nodes, term 2, leader has 7 entries, follower has 3 good and 3 spurious entries
RAFT_TEST_CASE(simple_3_follower_4_1, (test_case{
         .nodes = 3, .initial_term = 3,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}},
                            {.le = {{1,0},{1,1},{1,2},{2,20},{2,30},{2,40}}}},
         .updates = {entries{4}}}));

// A follower and a leader have matching logs but leader's is shorter
// 3 nodes, term 2, leader has 2 entries, follower has same and 5 more, 12 updates
RAFT_TEST_CASE(simple_3_short_leader, (test_case{
         .nodes = 3, .initial_term = 3,
         .initial_states = {{.le = {{1,0},{1,1}}},
                            {.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}}},
         .updates = {entries{12}}}));

// A follower and a leader have no common entries
// 3 nodes, term 2, leader has 7 entries, follower has non-matching 6 entries, 12 updates
RAFT_TEST_CASE(follower_not_matching, (test_case{
         .nodes = 3, .initial_term = 3,
         .initial_states = {{.le = {{1,0},{1,1},{1,2},{1,3},{1,4},{1,5},{1,6}}},
                            {.le = {{2,10},{2,20},{2,30},{2,40},{2,50},{2,60}}}},
         .updates = {entries{12}},}));

// A follower and a leader have one common entry
// 3 nodes, term 2, leader has 3 entries, follower has non-matching 3 entries, 12 updates
RAFT_TEST_CASE(follower_one_common_1, (test_case{
         .nodes = 3, .initial_term = 4,
         .initial_states = {{.le = {{1,0},{1,1},{1,2}}},
                            {.le = {{1,0},{2,11},{2,12},{2,13}}}},
         .updates = {entries{12}}}));

// A follower and a leader have 2 common entries in different terms
// 3 nodes, term 2, leader has 4 entries, follower has matching but in different term
RAFT_TEST_CASE(follower_one_common_2, (test_case{
         .nodes = 3, .initial_term = 5,
         .initial_states = {{.le = {{1,0},{2,1},{3,2},{3,3}}},
                            {.le = {{1,0},{2,1},{2,2},{2,13}}}},
         .updates = {entries{4}}}));

// 2 nodes both taking snapshot while simple replication
RAFT_TEST_CASE(take_snapshot, (test_case{
         .nodes = 2,
         .config = {{.snapshot_threshold = 10, .snapshot_trailing = 5}, {.snapshot_threshold = 20, .snapshot_trailing = 10}},
         .updates = {entries{100}}}));

// 2 nodes doing simple replication/snapshoting while leader's log size is limited
RAFT_TEST_CASE(backpressure, (test_case{
         .type = sm_type::SUM, .nodes = 2,
         .config = {{.snapshot_threshold = 10, .snapshot_trailing = 5, .max_log_size = 20}, {.snapshot_threshold = 20, .snapshot_trailing = 10}},
         .updates = {entries{100}}}));

// 3 nodes, add entries, drop leader 0, add entries [implicit re-join all]
RAFT_TEST_CASE(drops_01, (test_case{
         .nodes = 3,
         .updates = {entries{4},partition{1,2},entries{4}}}));

// 3 nodes, add entries, drop follower 1, add entries [implicit re-join all]
RAFT_TEST_CASE(drops_02, (test_case{
         .nodes = 3,
         .updates = {entries{4},partition{0,2},entries{4},partition{2,1}}}));

// 3 nodes, add entries, drop leader 0, custom leader, add entries [implicit re-join all]
RAFT_TEST_CASE(drops_03, (test_case{
         .nodes = 3,
         .updates = {entries{4},partition{leader{1},2},entries{4}}}));

// 4 nodes, add entries, drop follower 1, custom leader, add entries [implicit re-join all]
RAFT_TEST_CASE(drops_04, (test_case{
         .nodes = 4,
         .updates = {entries{4},partition{0,2,3},entries{4},partition{1,leader{2},3}}}));

// TODO: change to RAFT_TEST_CASE once it's stable for handling packet drops
SEASTAR_THREAD_TEST_CASE(test_take_snapshot_and_stream) {
    replication_test(
        // Snapshot automatic take and load
        {.nodes = 3,
         .config = {{.snapshot_threshold = 10, .snapshot_trailing = 5}},
         .updates = {entries{5}, partition{0,1}, entries{10}, partition{0, 2}, entries{20}}}
    , false);
}

// verifies that each node in a cluster can campaign
// and be elected in turn. This ensures that elections work when not
// starting from a clean slate (as they do in TestLeaderElection)
// TODO: add pre-vote case
RAFT_TEST_CASE(etcd_test_leader_cycle, (test_case{
         .nodes = 3,
         .updates = {new_leader{1},new_leader{2},new_leader{0}}}));

