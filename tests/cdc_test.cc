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

#include <seastar/testing/thread_test_case.hh>

#include "tests/cql_test_env.hh"

SEASTAR_THREAD_TEST_CASE(test_with_cdc_parameter) {
    do_with_cql_env_thread([](cql_test_env& e) {
        auto assert_cdc = [&] (bool set) {
            BOOST_REQUIRE_EQUAL(set, e.local_db().find_schema("ks", "tbl")->cdc_enabled());
        };
        auto alter_table_and_assert = [&] (bool cdc) {
            e.execute_cql(format("ALTER TABLE ks.tbl WITH cdc = '{}'", cdc)).get();
            assert_cdc(cdc);
        };
        // Create a table using given create statement and then check that
        // cdc property is set as expected. Then alter the table to set
        // cdc property to an opposite value and finally alter it again to
        // set it back to the initial value.
        auto test = [&] (const sstring& create_stmt, bool expected_cdc_value) {
            e.execute_cql(create_stmt).get();
            assert_cdc(expected_cdc_value);
            alter_table_and_assert(!expected_cdc_value);
            alter_table_and_assert(expected_cdc_value);
            e.execute_cql("DROP TABLE ks.tbl").get();
        };
        test("CREATE TABLE ks.tbl (a int PRIMARY KEY)", false);
        test("CREATE TABLE ks.tbl (a int PRIMARY KEY) WITH cdc = 'false'", false);
        test("CREATE TABLE ks.tbl (a int PRIMARY KEY) WITH cdc = 'true'", true);
    }).get();
}

