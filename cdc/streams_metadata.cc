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
 *
 * Modified by ScyllaDB
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

namespace cdc {

void streams_metadata::update(inet_address ep, const std::vector<utils:UUID>& ss) {
    _streams.insert_or_assign(ep, ss);
}

void streams_metadata::remove(inet_address ep) {
    _streams.remove(ep);
}

utils::UUID stream_for_token(dht::token t, const token_metadata& tm, unsigned sharding_ignore_msb_bits) {
    auto ep = tm.get_endpoint(tm.first_token(t));
    auto it = _streams.find(ep);
    if (it == _streams.end() || it->second.empty()) {
        // This is a bug. We should always make sure that every known endpoint has at least one stream.
        throw std::runtime_error(format("No CDC streams found for endpoint {}, which owns token {}.", ep, t));
    }

    auto& streams = it->second;

    // We assume that:
    // 1. there are as many shards on `ep` as there are streams that `ep` has informed us about,
    // 2. the `i`th stream in the list sent by `ep` is owned by the `i`th shard on node `ep`,
    // 3. `ep` is the primary replica for `t`,
    // 4. the same value for `murmur3_partitioner_ignore_msb_bits is used for all nodes in the cluster
    //      (and is given in the `sharding_ignore_msb_bits` parameter).
    // In that case we will choose the stream ID so that the CDC mutation will be sent to the same replicas
    // as the corresponding base table mutation, and also to the same shard on the primary replica.

    // The assumptions don't always hold, but that's fine. We don't loose correctness, only performance (possibly).
    // However:
    // a) if there are no further changes in the cluster (no nodes joining/leaving and no resharding performed),
    // assumptions 1 and 2 will eventually hold,
    // b) at the moment of writing this comment, assumption 3 can only be false when NetworkTopologyStrategy is used
    // and `ep` belongs to a datacenter with RF = 0, which should be very uncommon,
    // c) assumption 4 is to be taken care of the administrator, but there are no reasons to configure the cluster
    // to use different values of this parameter on different nodes.

    auto s = dht::murmur3_partitioner::shard_of(t, streams.size(), sharding_ignore_msb_bits);
    assert(s < streams.size());
    return streams[s];
}

} // namespace cdc
