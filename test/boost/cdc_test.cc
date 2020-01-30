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
#include <string>

#include "cdc/log.hh"
#include "schema_builder.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"
#include "transport/messages/result_message.hh"

#include "types.hh"
#include "types/tuple.hh"
#include "types/map.hh"
#include "types/user.hh"

using namespace std::string_literals;

namespace cdc {
api::timestamp_type find_timestamp(const schema&, const mutation&);
}

SEASTAR_THREAD_TEST_CASE(test_find_mutation_timestamp) {
    do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "CREATE TYPE ut (a int, b int)");
        cquery_nofail(e, "CREATE TABLE ks.t (pk int, ck int, vstatic int static, vint int, "
                "vmap map<int, int>, vfmap frozen<map<int, int>>, vut ut, vfut frozen<ut>, primary key (pk, ck))");

        auto schema = schema_builder("ks", "t")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("vstatic", int32_type, column_kind::static_column)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("vint", int32_type)
            .with_column("vmap", map_type_impl::get_instance(int32_type, int32_type, true))
            .with_column("vfmap", map_type_impl::get_instance(int32_type, int32_type, false))
            .with_column("vut", user_type_impl::get_instance("ks", "ut", {to_bytes("a"), to_bytes("b")}, {int32_type, int32_type}, true))
            .with_column("vfut", user_type_impl::get_instance("ks", "ut", {to_bytes("a"), to_bytes("b")}, {int32_type, int32_type}, false))
            .build();

        auto check_stmt = [&] (const sstring& query) {
            auto muts = e.get_modification_mutations(query).get0();
            BOOST_REQUIRE(!muts.empty());

            for (auto& m: muts) {
                /* We want to check if `find_timestamp` truly returns this mutation's timestamp.
                 * The mutation was created very recently (in the `get_modification_mutations` call),
                 * so we can do it by comparing the returned timestamp with the current time
                 * -- the difference should be small.
                 */
                auto ts = cdc::find_timestamp(*schema, m);
                BOOST_REQUIRE(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        api::timestamp_clock::duration(api::new_timestamp() - ts))
                    < std::chrono::milliseconds(5000));
            }
        };

        check_stmt("INSERT INTO ks.t (pk, ck) VALUES (0, 0)");
        check_stmt("INSERT INTO ks.t (pk, ck, vint) VALUES (0,0,0)");
        check_stmt("INSERT INTO ks.t (pk, ck, vmap) VALUES (0,0,{0:0})");
        check_stmt("INSERT INTO ks.t (pk, ck, vfmap) VALUES (0,0,{0:0})");
        check_stmt("INSERT INTO ks.t (pk, ck, vut) VALUES (0,0,{b:0})");
        check_stmt("INSERT INTO ks.t (pk, ck, vfut) VALUES (0,0,{b:0})");
        check_stmt("INSERT INTO ks.t (pk, ck, vint) VALUES (0,0,null)");
        check_stmt("INSERT INTO ks.t (pk, ck, vmap) VALUES (0,0,null)");
        check_stmt("INSERT INTO ks.t (pk, ck, vfmap) VALUES (0,0,null)");
        check_stmt("INSERT INTO ks.t (pk, ck, vut) VALUES (0,0,null)");
        check_stmt("INSERT INTO ks.t (pk, ck, vfut) VALUES (0,0,null)");
        check_stmt("INSERT INTO ks.t (pk, vstatic) VALUES (0, 0)");
        check_stmt("UPDATE ks.t SET vint = 0 WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vmap = {0:0} WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vmap[0] = 0 WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vfmap = {0:0} WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vut = {b:0} WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vut.b = 0 WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vfut = {b:0} WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vint = null WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vmap = null WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vmap[0] = null WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vfmap = null WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vut = null WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vut.b = null WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vfut = null WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vstatic = 0 WHERE pk = 0");
        check_stmt("DELETE FROM ks.t WHERE pk = 0 and ck = 0");
        check_stmt("DELETE FROM ks.t WHERE pk = 0 and ck > 0");
        check_stmt("DELETE FROM ks.t WHERE pk = 0 and ck > 0 and ck <= 1");
        check_stmt("DELETE FROM ks.t WHERE pk = 0");
        check_stmt("DELETE vint FROM t WHERE pk = 0 AND ck = 0");
        check_stmt("DELETE vmap FROM t WHERE pk = 0 AND ck = 0");
        check_stmt("DELETE vmap[0] FROM t WHERE pk = 0 AND ck = 0");
        check_stmt("DELETE vfmap FROM t WHERE pk = 0 AND ck = 0");
        check_stmt("DELETE vut FROM t WHERE pk = 0 AND ck = 0");
        check_stmt("DELETE vut.b FROM t WHERE pk = 0 AND ck = 0");
        check_stmt("DELETE vfut FROM t WHERE pk = 0 AND ck = 0");
        check_stmt("DELETE vstatic FROM t WHERE pk = 0");
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_with_cdc_parameter) {
    do_with_cql_env_thread([](cql_test_env& e) {
        struct expected {
            bool enabled = false;
            bool preimage = false;
            bool postimage = false;
            int ttl = 86400;
        };

        auto assert_cdc = [&] (const expected& exp) {
            BOOST_REQUIRE_EQUAL(exp.enabled,
                    e.local_db().find_schema("ks", "tbl")->cdc_options().enabled());
            if (exp.enabled) {
                e.require_table_exists("ks", cdc::log_name("tbl")).get();
            } else {
                e.require_table_does_not_exist("ks", cdc::log_name("tbl")).get();
            }
            BOOST_REQUIRE_EQUAL(exp.preimage,
                    e.local_db().find_schema("ks", "tbl")->cdc_options().preimage());
            BOOST_REQUIRE_EQUAL(exp.postimage,
                    e.local_db().find_schema("ks", "tbl")->cdc_options().postimage());
            BOOST_REQUIRE_EQUAL(exp.ttl,
                    e.local_db().find_schema("ks", "tbl")->cdc_options().ttl());
        };

        auto test = [&] (const sstring& create_prop,
                         const sstring& alter1_prop,
                         const sstring& alter2_prop,
                         const expected& create_expected,
                         const expected& alter1_expected,
                         const expected& alter2_expected) {
            e.execute_cql(format("CREATE TABLE ks.tbl (a int PRIMARY KEY) {}", create_prop)).get();
            assert_cdc(create_expected);
            e.execute_cql(format("ALTER TABLE ks.tbl WITH cdc = {}", alter1_prop)).get();
            assert_cdc(alter1_expected);
            e.execute_cql(format("ALTER TABLE ks.tbl WITH cdc = {}", alter2_prop)).get();
            assert_cdc(alter2_expected);
            e.execute_cql("DROP TABLE ks.tbl").get();
            e.require_table_does_not_exist("ks", cdc::log_name("tbl")).get();
        };

        test("", "{'enabled':'true'}", "{'enabled':'false'}", {false}, {true}, {false});
        test("WITH cdc = {'enabled':'true'}", "{'enabled':'false'}", "{'enabled':'true'}", {true}, {false}, {true});
        test("WITH cdc = {'enabled':'false'}", "{'enabled':'true'}", "{'enabled':'false'}", {false}, {true}, {false});
        test("", "{'enabled':'true','preimage':'true','postimage':'true','ttl':'1'}", "{'enabled':'false'}", {false}, {true, true, true, 1}, {false});
        test("WITH cdc = {'enabled':'true','preimage':'true','postimage':'true','ttl':'1'}", "{'enabled':'false'}", "{'enabled':'true','preimage':'false','postimage':'true','ttl':'2'}", {true, true, true, 1}, {false}, {true, false, true, 2});
    }).get();
}

static std::vector<std::vector<bytes_opt>> to_bytes(const cql_transport::messages::result_message::rows& rows) {
    auto rs = rows.rs().result_set().rows();
    std::vector<std::vector<bytes_opt>> results;
    for (auto it = rs.begin(); it != rs.end(); ++it) {
        results.push_back(*it);
    }
    return results;
}

static size_t column_index(const cql_transport::messages::result_message::rows& rows, const sstring& name) {
    size_t res = 0;
    for (auto& col : rows.rs().get_metadata().get_names()) {
        if (col->name->text() == name) {
            return res;
        }
        ++res;
    }
    throw std::invalid_argument("No such column: " + name);
}

template<typename Comp = std::equal_to<bytes_opt>>
static std::vector<std::vector<bytes_opt>> to_bytes_filtered(const cql_transport::messages::result_message::rows& rows, cdc::operation op, const Comp& comp = {}) {
    const auto op_type = data_type_for<std::underlying_type_t<cdc::operation>>();

    auto results = to_bytes(rows);
    auto op_index = column_index(rows, "operation");
    auto op_bytes = op_type->decompose(std::underlying_type_t<cdc::operation>(op));

    results.erase(std::remove_if(results.begin(), results.end(), [&](const std::vector<bytes_opt>& bo) {
        return !comp(op_bytes, bo[op_index]);
    }), results.end());

    return results;
}

static void sort_by_time(const cql_transport::messages::result_message::rows& rows, std::vector<std::vector<bytes_opt>>& results) {
    auto time_index = column_index(rows, "time");
    std::sort(results.begin(), results.end(),
            [time_index] (const std::vector<bytes_opt>& a, const std::vector<bytes_opt>& b) {
                return timeuuid_type->as_less_comparator()(*a[time_index], *b[time_index]);
            });
}

static auto select_log(cql_test_env& e, const sstring& table_name) {
    auto msg = e.execute_cql(format("SELECT * FROM ks.{}", cdc::log_name(table_name))).get0();
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
    BOOST_REQUIRE(rows);
    return rows;
};

SEASTAR_THREAD_TEST_CASE(test_primary_key_logging) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, pk2 int, ck int, ck2 int, val int, PRIMARY KEY((pk, pk2), ck, ck2)) WITH cdc = {'enabled':'true'}");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 11, 111, 1111, 11111)");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 22, 222, 2222, 22222)");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 33, 333, 3333, 33333)");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 44, 444, 4444, 44444)");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(2, 11, 111, 1111, 11111)");
        cquery_nofail(e, "DELETE val FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck = 111 AND ck2 = 1111");
        cquery_nofail(e, "DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck = 111 AND ck2 = 1111");
        cquery_nofail(e, "DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck > 222 AND ck <= 444");
        cquery_nofail(e, "UPDATE ks.tbl SET val = 555 WHERE pk = 2 AND pk2 = 11 AND ck = 111 AND ck2 = 1111");
        cquery_nofail(e, "DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11");
        auto msg = e.execute_cql(format("SELECT time, \"_pk\", \"_pk2\", \"_ck\", \"_ck2\", operation FROM ks.{}", cdc::log_name("tbl"))).get0();
        auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
        BOOST_REQUIRE(rows);

        auto results = to_bytes_filtered(*rows, cdc::operation::pre_image, std::not_equal_to<bytes_opt>{});

        sort_by_time(*rows, results);

        auto actual_i = results.begin();
        auto actual_end = results.end();
        auto assert_row = [&] (int pk, int pk2, int ck = -1, int ck2 = -1) {
            std::cerr << "check " << pk << " " << pk2 << " " << ck << " " << ck2 << std::endl;
            BOOST_REQUIRE(actual_i != actual_end);
            auto& actual_row = *actual_i;
            BOOST_REQUIRE_EQUAL(int32_type->decompose(pk), actual_row[1]);
            BOOST_REQUIRE_EQUAL(int32_type->decompose(pk2), actual_row[2]);
            if (ck != -1) {
                BOOST_REQUIRE_EQUAL(int32_type->decompose(ck), actual_row[3]);
            } else {
                BOOST_REQUIRE(!actual_row[3]);
            }
            if (ck2 != -1) {
                BOOST_REQUIRE_EQUAL(int32_type->decompose(ck2), actual_row[4]);
            } else {
                BOOST_REQUIRE(!actual_row[4]);
            }
            ++actual_i;
        };
        // INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 11, 111, 1111, 11111)
        assert_row(1, 11, 111, 1111);
        // INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 22, 222, 2222, 22222)
        assert_row(1, 22, 222, 2222);
        // INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 33, 333, 3333, 33333)
        assert_row(1, 33, 333, 3333);
        // INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 44, 444, 4444, 44444)
        assert_row(1, 44, 444, 4444);
        // INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(2, 11, 111, 1111, 11111)
        assert_row(2, 11, 111, 1111);
        // DELETE val FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck = 111 AND ck2 = 1111
        assert_row(1, 11, 111, 1111);
        // DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck = 111 AND ck2 = 1111
        assert_row(1, 11, 111, 1111);
        // First row for DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck > 222 AND ck <= 444
        assert_row(1, 11, 222);
        // Second row for DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck > 222 AND ck <= 444
        assert_row(1, 11, 444);
        // UPDATE ks.tbl SET val = 555 WHERE pk = 2 AND pk2 = 11 AND ck = 111 AND ck2 = 1111
        assert_row(2, 11, 111, 1111);
        // DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11
        assert_row(1, 11);
        BOOST_REQUIRE(actual_i == actual_end);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_pre_image_logging) {
    do_with_cql_env_thread([](cql_test_env& e) {
        auto test = [&e] (bool enabled) {
            cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, pk2 int, ck int, ck2 int, val int, PRIMARY KEY((pk, pk2), ck, ck2)) WITH cdc = {'enabled':'true', 'preimage':'"s + (enabled ? "true" : "false") + "'}");
            cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 11, 111, 1111, 11111)");

            auto rows = select_log(e, "tbl");

            BOOST_REQUIRE(to_bytes_filtered(*rows, cdc::operation::pre_image).empty());

            auto first = to_bytes_filtered(*rows, cdc::operation::update);

            auto ck2_index = column_index(*rows, "_ck2");
            auto val_index = column_index(*rows, "_val");

            auto val_type = tuple_type_impl::get_instance({ data_type_for<std::underlying_type_t<cdc::column_op>>(), int32_type, long_type});
            auto val = *first[0][val_index];

            BOOST_REQUIRE_EQUAL(int32_type->decompose(1111), first[0][ck2_index]);
            BOOST_REQUIRE_EQUAL(data_value(11111), value_cast<tuple_type_impl::native_type>(val_type->deserialize(bytes_view(val))).at(1));

            auto last = 11111;
            for (auto i = 0u; i < 10; ++i) {
                auto nv = last + 1;
                cquery_nofail(e, "UPDATE ks.tbl SET val=" + std::to_string(nv) +" where pk=1 AND pk2=11 AND ck=111 AND ck2=1111");

                rows = select_log(e, "tbl");

                auto pre_image = to_bytes_filtered(*rows, cdc::operation::pre_image);
                auto second = to_bytes_filtered(*rows, cdc::operation::update);

                if (!enabled) {
                    BOOST_REQUIRE(pre_image.empty());
                } else {
                    sort_by_time(*rows, second);
                    BOOST_REQUIRE_EQUAL(pre_image.size(), i + 1);

                    val = *pre_image.back()[val_index];
                    BOOST_REQUIRE_EQUAL(int32_type->decompose(1111), pre_image[0][ck2_index]);
                    BOOST_REQUIRE_EQUAL(data_value(last), value_cast<tuple_type_impl::native_type>(val_type->deserialize(bytes_view(val))).at(1));
                }

                last = nv;
            }
            e.execute_cql("DROP TABLE ks.tbl").get();
        };
        test(true);
        test(false);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_add_columns) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, pk2 int, ck int, ck2 int, val int, PRIMARY KEY((pk, pk2), ck, ck2)) WITH cdc = {'enabled':'true'}");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 11, 111, 1111, 11111)");

        auto rows = select_log(e, "tbl");

        BOOST_REQUIRE(!to_bytes_filtered(*rows, cdc::operation::update).empty());

        cquery_nofail(e, "ALTER TABLE ks.tbl ADD kokos text");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val, kokos) VALUES(1, 11, 111, 1111, 11111, 'kaka')");

        rows = select_log(e, "tbl");
        auto updates = to_bytes_filtered(*rows, cdc::operation::update);
        sort_by_time(*rows, updates);

        auto kokos_index = column_index(*rows, "_kokos");
        auto kokos_type = tuple_type_impl::get_instance({ data_type_for<std::underlying_type_t<cdc::column_op>>(), utf8_type, long_type});
        auto kokos = *updates.back()[kokos_index];

        BOOST_REQUIRE_EQUAL(data_value("kaka"), value_cast<tuple_type_impl::native_type>(kokos_type->deserialize(bytes_view(kokos))).at(1));
    }).get();
}

// #5582 - just quickly test that we can create the cdc enabled table on a different shard 
// and still get the logs proper. 
SEASTAR_THREAD_TEST_CASE(test_cdc_across_shards) {
    do_with_cql_env_thread([](cql_test_env& e) {
        smp::submit_to(1, [&e] {
            // this is actually ok. there is no members of cql_test_env actually used in call
            return seastar::async([&] {
                cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, pk2 int, ck int, ck2 int, val int, PRIMARY KEY((pk, pk2), ck, ck2)) WITH cdc = {'enabled':'true'}");
            });
        }).get();
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 11, 111, 1111, 11111)");

        auto rows = select_log(e, "tbl");

        BOOST_REQUIRE(!to_bytes_filtered(*rows, cdc::operation::update).empty());
    }).get();
}
