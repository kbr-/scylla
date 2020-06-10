/*
 * Copyright (C) 2020 ScyllaDB
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

#include "service/storage_proxy.hh"

#include "kafka_upload_service.hh"
#include <kafka4seastar/protocol/metadata_response.hh>

using namespace kafka4seastar;

namespace cdc::kafka {

void kafka_upload_service::on_timer() {
    arm_timer();

    // Logic goes here. Remember to wait for it to finish in
    // kafka_upload_service::stop
    metadata_response_broker broker {kafka_int32_t(0), kafka_string_t("host"), kafka_int32_t(1234), {}};
    broker.serialize(std::cout, 666);
}

void kafka_upload_service::arm_timer() {
    _timer.arm(seastar::lowres_clock::now() + std::chrono::seconds(10));
}

kafka_upload_service::kafka_upload_service(service::storage_proxy& proxy)
    : _proxy(proxy)
    , _timer([this] { on_timer(); })
{
    _proxy.set_kafka_upload_service(this);
    arm_timer();
}

future<> kafka_upload_service::stop() {
    _timer.cancel();
    return make_ready_future<>();
}

} // namespace cdc::kafka
