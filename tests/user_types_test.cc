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

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"

#include "types/user.hh"
#include "exception_utils.hh"

// TODO kbr: copy paste
static void flush(cql_test_env& e) {
    e.db().invoke_on_all([](database& dbi) {
        return dbi.flush_all_memtables();
    }).get();
}

SEASTAR_TEST_CASE(test_user_type_nested) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create type ut1 (f1 int);").get();
        e.execute_cql("create type ut2 (f2 frozen<ut1>);").get();
    });
}

SEASTAR_TEST_CASE(test_user_type_reversed) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create type my_type (a int);").get();
        e.execute_cql("create table tbl (a int, b frozen<my_type>, primary key ((a), b)) with clustering order by (b desc);").get();
        e.execute_cql("insert into tbl (a, b) values (1, (2));").get();
        assert_that(e.execute_cql("select a,b.a from tbl;").get0())
                .is_rows()
                .with_size(1)
                .with_row({int32_type->decompose(1), int32_type->decompose(2)});
    });
}

SEASTAR_TEST_CASE(test_user_type) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create type ut1 (my_int int, my_bigint bigint, my_text text);").discard_result().then([&e] {
            return e.execute_cql("create table cf (id int primary key, t frozen <ut1>);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into cf (id, t) values (1, (1001, 2001, 'abc1'));").discard_result();
        }).then([&e] {
            return e.execute_cql("select t.my_int, t.my_bigint, t.my_text from cf where id = 1;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_rows({
                     {int32_type->decompose(int32_t(1001)), long_type->decompose(int64_t(2001)), utf8_type->decompose(sstring("abc1"))},
                });
        }).then([&e] {
            return e.execute_cql("update cf set t = { my_int: 1002, my_bigint: 2002, my_text: 'abc2' } where id = 1;").discard_result();
        }).then([&e] {
            return e.execute_cql("select t.my_int, t.my_bigint, t.my_text from cf where id = 1;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_rows({
                     {int32_type->decompose(int32_t(1002)), long_type->decompose(int64_t(2002)), utf8_type->decompose(sstring("abc2"))},
                });
        }).then([&e] {
            return e.execute_cql("insert into cf (id, t) values (2, (frozen<ut1>)(2001, 3001, 'abc4'));").discard_result();
        }).then([&e] {
            return e.execute_cql("select t from cf where id = 2;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            auto ut = user_type_impl::get_instance("ks", to_bytes("ut1"),
                        {to_bytes("my_int"), to_bytes("my_bigint"), to_bytes("my_text")},
                        {int32_type, long_type, utf8_type}, false);
            auto ut_val = make_user_value(ut,
                          user_type_impl::native_type({int32_t(2001),
                                                       int64_t(3001),
                                                       sstring("abc4")}));
            assert_that(msg).is_rows()
                .with_rows({
                     {ut->decompose(ut_val)},
                });
        });
    });
}

// TODO kbr: move this to some header
// Specifies that the given 'cql' query fails with the 'msg' message.
// Requires a cql_test_env. The caller must be inside thread.
#define REQUIRE_INVALID(e, cql, msg) \
    BOOST_REQUIRE_EXCEPTION( \
        e.execute_cql(cql).get(), \
        exceptions::invalid_request_exception, \
        exception_predicate::message_equals(msg))

SEASTAR_TEST_CASE(test_invalid_user_type_statements) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create type ut1 (a int)").discard_result().get();

        // non-frozen UDTs can't be part of primary key
        REQUIRE_INVALID(e, "create table bad (a ut1 primary key, b int)",
                "Invalid non-frozen user-defined type for PRIMARY KEY component a");
        REQUIRE_INVALID(e, "create table bad (a int, b ut1, c int, primary key (a, b))",
                "Invalid non-frozen user-defined type for PRIMARY KEY component b");

        // non-frozen UDTs can't be inside collections, in create table statements...
        REQUIRE_INVALID(e, "create table bad (a int primary key, b list<ut1>)",
                "Non-frozen user types or collections are not allowed inside collections: list<ut1>");
        REQUIRE_INVALID(e, "create table bad (a int primary key, b set<ut1>)",
                "Non-frozen user types or collections are not allowed inside collections: set<ut1>");
        REQUIRE_INVALID(e, "create table bad (a int primary key, b map<int, ut1>)",
                "Non-frozen user types or collections are not allowed inside collections: map<int, ut1>");
        REQUIRE_INVALID(e, "create table bad (a int primary key, b map<ut1, int>)",
                "Non-frozen user types or collections are not allowed inside collections: map<ut1, int>");
        // ... and in user type definitions
        REQUIRE_INVALID(e, "create type ut2 (a int, b list<ut1>)",
                "Non-frozen user types or collections are not allowed inside collections: list<ut1>");
        //
        // non-frozen UDTs can't be inside UDTs
        REQUIRE_INVALID(e, "create type ut2 (a int, b ut1)",
                "A user type cannot contain non-frozen user type fields");

        // table cannot refer to UDT in another keyspace
        e.execute_cql("create keyspace ks2 with replication={'class':'SimpleStrategy','replication_factor':1}").discard_result().get();
        e.execute_cql("create type ks2.ut2 (a int)").discard_result().get();
        REQUIRE_INVALID(e, "create table bad (a int primary key, b ks2.ut2)",
                "Statement on keyspace ks cannot refer to a user type in keyspace ks2; "
                "user types can only be used in the keyspace they are defined in");
        REQUIRE_INVALID(e, "create table bad (a int primary key, b frozen<ks2.ut2>)",
                "Statement on keyspace ks cannot refer to a user type in keyspace ks2; "
                "user types can only be used in the keyspace they are defined in");

        // can't reference non-existing UDT
        REQUIRE_INVALID(e, "create table bad (a int primary key, b ut2)",
                "Unknown type ks.ut2");

        // can't delete fields of frozen UDT or non-UDT columns
        e.execute_cql("create table cf1 (a int primary key, b frozen<ut1>, c int)").discard_result().get();
        REQUIRE_INVALID(e, "delete b.a from cf1 where a = 0",
                "Frozen UDT column b does not support field deletions");
        REQUIRE_INVALID(e, "delete c.a from cf1 where a = 0",
                "Invalid deletion operation for non-UDT column c");

        // can't update fields of frozen UDT or non-UDT columns
        REQUIRE_INVALID(e, "update cf1 set b.a = 0 where a = 0",
                "Invalid operation (b.a = 0) for frozen UDT column b");
        REQUIRE_INVALID(e, "update cf1 set c.a = 0 where a = 0",
                "Invalid operation (c.a = 0) for non-UDT column c");

        // can't delete non-existing fields of UDT columns
        e.execute_cql("create table cf2 (a int primary key, b ut1, c int)").discard_result().get();
        REQUIRE_INVALID(e, "delete b.foo from cf2 where a = 0",
                "UDT column b does not have a field named foo");

        // can't update non-existing fields of UDT columns
        REQUIRE_INVALID(e, "update cf2 set b.foo = 0 where a = 0",
                "UDT column b does not have a field named foo");

        // can't insert UDT with non-existing fields
        REQUIRE_INVALID(e, "insert into cf2 (a, b, c) VALUES (0, {a:0,foo:0}, 0)",
                "Unknown field 'foo' in value of user defined type ut1");
        REQUIRE_INVALID(e, "insert into cf2 (a, b, c) VALUES (0, (0, 0), 0)",
                "Invalid tuple literal for b: too many elements. Type ut1 expects 1 but got 2");

        // non-frozen UDTs can't contain non-frozen collections
        e.execute_cql("create type ut3 (a int, b list<int>)").discard_result().get();
        REQUIRE_INVALID(e, "create table bad (a int primary key, b ut3)",
                "Non-frozen UDTs with nested non-frozen collections are not supported");
    });
}

template <typename F>
void before_and_after_flush(cql_test_env& e, F f) {
    f();
    flush(e);
    f();
}

static future<> test_alter_user_type(bool frozen) {
    return do_with_cql_env_thread([frozen] (cql_test_env& e) {
        const sstring val1 = "1";
        const sstring val2 = "22";
        const sstring val3 = "333";
        const sstring val4 = "4444";

        e.execute_cql("create type ut1 (b text)").discard_result().get();
        e.execute_cql(format("create table cf1 (a int primary key, b {})", frozen ? "frozen<ut1>" : "ut1")).discard_result().get();
        e.execute_cql("insert into cf1 (a, b) values (1, {b:'1'})").discard_result().get();

        auto msg = e.execute_cql("select b.b from cf1").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                {{utf8_type->decompose(val1)}},
        });

        e.execute_cql("alter type ut1 add a int").discard_result().get();
        e.execute_cql("insert into cf1 (a, b) values (2, {a:2,b:'22'})").discard_result().get();

        auto ut1 = user_type_impl::get_instance("ks", to_bytes("ut1"),
                    {to_bytes("b"), to_bytes("a")}, {utf8_type, int32_type}, !frozen);

        msg = e.execute_cql("select * from cf1").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
            {int32_type->decompose(1), ut1->decompose(make_user_value(ut1, user_type_impl::native_type(
                            {val1})))},
            {int32_type->decompose(2), ut1->decompose(make_user_value(ut1, user_type_impl::native_type(
                            {val2, 2})))},
        });

        msg = e.execute_cql("select * from cf1 where b={b:'1'} allow filtering").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
            {int32_type->decompose(1), ut1->decompose(make_user_value(ut1, user_type_impl::native_type(
                            {val1})))},
        });

        msg = e.execute_cql("select b.a from cf1").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
            {{}},
            {{int32_type->decompose(2)}},
        });

        flush(e);

        e.execute_cql("alter type ut1 add c int").discard_result().get();

        ut1 = user_type_impl::get_instance("ks", to_bytes("ut1"),
                    {to_bytes("b"), to_bytes("a"), to_bytes("c")}, {utf8_type, int32_type, int32_type}, !frozen);

        msg = e.execute_cql("select * from cf1").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
            {int32_type->decompose(1), ut1->decompose(make_user_value(ut1, user_type_impl::native_type(
                            {val1})))},
            {int32_type->decompose(2), ut1->decompose(make_user_value(ut1, user_type_impl::native_type(
                            {val2, 2})))},
        });

        e.execute_cql("alter type ut1 rename b to foo").discard_result().get();

        ut1 = user_type_impl::get_instance("ks", to_bytes("ut1"),
                    {to_bytes("foo"), to_bytes("a"), to_bytes("c")}, {utf8_type, int32_type, int32_type}, !frozen);

        e.execute_cql("insert into cf1 (a, b) values (3, ('333', 3, 3))").discard_result().get();
        e.execute_cql("insert into cf1 (a, b) values (4, {foo:'4444',c:4})").discard_result().get();

        before_and_after_flush(e, [&] {
            msg = e.execute_cql("select * from cf1").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                {int32_type->decompose(1), ut1->decompose(make_user_value(ut1, user_type_impl::native_type(
                                {val1})))},
                {int32_type->decompose(2), ut1->decompose(make_user_value(ut1, user_type_impl::native_type(
                                {val2, 2})))},
                {int32_type->decompose(3), ut1->decompose(make_user_value(ut1, user_type_impl::native_type(
                                {val3, 3, 3})))},
                {int32_type->decompose(4), ut1->decompose(make_user_value(ut1, user_type_impl::native_type(
                                {val4, data_value::make_null(int32_type), 4})))},
            });

            msg = e.execute_cql("select b.foo from cf1").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                {utf8_type->decompose(val1)},
                {utf8_type->decompose(val2)},
                {utf8_type->decompose(val3)},
                {utf8_type->decompose(val4)},
            });
        });
        // TODO kbr: remove field?
    });
}

SEASTAR_TEST_CASE(test_alter_frozen_user_type) {
    return test_alter_user_type(true);
}

SEASTAR_TEST_CASE(test_alter_nonfrozen_user_type) {
    return test_alter_user_type(false);
}
