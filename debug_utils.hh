#pragma once

#include <ostream>
#include <seastar/core/sstring.hh>

using namespace seastar;

extern logging::logger cdc_log;

namespace cdc {

class stream_id;
class topology_description;

std::ostream& operator<<(std::ostream&, const stream_id&);
std::ostream& operator<<(std::ostream&, const topology_description&);

} //namespace cdc

namespace locator {
    class token_metadata;
}

sstring print_token_owners(const locator::token_metadata&);
