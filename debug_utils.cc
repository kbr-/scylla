#include <seastar/core/print.hh>
#include <set>

#include "log.hh"
#include "dht/i_partitioner.hh"
#include "gms/inet_address.hh"
#include "locator/token_metadata.hh"

#include "cdc/generation.hh"
#include "debug_utils.hh"

using gms::inet_address;
using locator::token_metadata;

namespace cdc {

std::ostream& operator<<(std::ostream& os, const stream_id& sid) {
    return os << format("stream_id({}, {})", sid.first(), sid.second());
}

std::ostream& operator<<(std::ostream& os, const topology_description& desc) {
    std::vector<dht::token> tokens;
    std::vector<std::vector<stream_id>> streams;
    std::vector<uint64_t> sharding_ignore_msb;

    for (auto& e: desc.entries()) {
        tokens.push_back(e.token_range_end);
        streams.push_back(e.streams);
        sharding_ignore_msb.push_back(e.sharding_ignore_msb);
    }

    return os << format(
            "\n--- cluster topology description ---\ntokens:\n{}\nstreams:\n{}\nsharding_ignore_msb:\n{}\n---",
            tokens, streams, sharding_ignore_msb);
}

}

sstring print_token_owners(const token_metadata& tm) {
    std::set<inet_address> ret;
    auto& tok_to_ep = tm.get_token_to_endpoint();
    for (auto [t, ep]: tok_to_ep) {
        ret.insert(ep);
    }
    return format("{}", ret);
}
