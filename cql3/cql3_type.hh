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
 */

/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015 ScyllaDB
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

#pragma once

#include "types.hh"
#include "exceptions/exceptions.hh"
#include <iosfwd>
#include "enum_set.hh"

class database;
class user_types_metadata;

namespace cql3 {

class ut_name;

class cql3_type final {
    data_type _type;
public:
    cql3_type(data_type type) : _type(std::move(type)) {}
    bool is_collection() const { return _type->is_collection(); }
    bool is_counter() const { return _type->is_counter(); }
    bool is_native() const { return _type->is_native(); }
    bool is_user_type() const { return _type->is_user_type(); }
    data_type get_type() const { return _type; }
    const sstring& to_string() const { return _type->cql3_type_name(); }

    // For UserTypes, we need to know the current keyspace to resolve the
    // actual type used, so Raw is a "not yet prepared" CQL3Type.
    class raw {
        virtual sstring to_string() const = 0;
    protected:
        bool _frozen = false;
    public:
        virtual ~raw() {}
        virtual bool supports_freezing() const = 0;
        virtual bool is_collection() const;
        virtual bool is_counter() const;
        virtual bool is_duration() const;
        virtual bool is_user_type() const;
        bool is_frozen() const;
        virtual bool references_user_type(const sstring&) const;
        virtual std::optional<sstring> keyspace() const;
        virtual void freeze();
        virtual cql3_type prepare_internal(const sstring& keyspace, lw_shared_ptr<user_types_metadata>) = 0;
        virtual cql3_type prepare(database& db, const sstring& keyspace);
        static shared_ptr<raw> from(cql3_type type);
        static shared_ptr<raw> user_type(ut_name name);
        static shared_ptr<raw> map(shared_ptr<raw> t1, shared_ptr<raw> t2);
        static shared_ptr<raw> list(shared_ptr<raw> t);
        static shared_ptr<raw> set(shared_ptr<raw> t);
        static shared_ptr<raw> tuple(std::vector<shared_ptr<raw>> ts);
        static shared_ptr<raw> frozen(shared_ptr<raw> t);
        friend std::ostream& operator<<(std::ostream& os, const raw& r);
    };

private:
    class raw_type;
    class raw_collection;
    class raw_ut;
    class raw_tuple;
    friend std::ostream& operator<<(std::ostream& os, const cql3_type& t) {
        return os << t.to_string();
    }

public:
    static thread_local cql3_type ascii;
    static thread_local cql3_type bigint;
    static thread_local cql3_type blob;
    static thread_local cql3_type boolean;
    static thread_local cql3_type double_;
    static thread_local cql3_type empty;
    static thread_local cql3_type float_;
    static thread_local cql3_type int_;
    static thread_local cql3_type smallint;
    static thread_local cql3_type text;
    static thread_local cql3_type timestamp;
    static thread_local cql3_type tinyint;
    static thread_local cql3_type uuid;
    static thread_local cql3_type timeuuid;
    static thread_local cql3_type date;
    static thread_local cql3_type time;
    static thread_local cql3_type inet;
    static thread_local cql3_type varint;
    static thread_local cql3_type decimal;
    static thread_local cql3_type counter;
    static thread_local cql3_type duration;

    static const std::vector<cql3_type>& values();
public:
    using kind = abstract_type::cql3_kind;
    using kind_enum_set = abstract_type::cql3_kind_enum_set;
    kind_enum_set::prepared get_kind() const { return _type->get_cql3_kind(); }
};

inline bool operator==(const cql3_type& a, const cql3_type& b) {
    return a.get_type() == b.get_type();
}

#if 0
    public static class Custom implements CQL3Type
    {
        private final AbstractType<?> type;

        public Custom(AbstractType<?> type)
        {
            this.type = type;
        }

        public Custom(String className) throws SyntaxException, ConfigurationException
        {
            this(TypeParser.parse(className));
        }

        public boolean isCollection()
        {
            return false;
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof Custom))
                return false;

            Custom that = (Custom)o;
            return type.equals(that.type);
        }

        @Override
        public final int hashCode()
        {
            return type.hashCode();
        }

        @Override
        public String toString()
        {
            return "'" + type + "'";
        }
    }

    public static class Collection implements CQL3Type
    {
        private final CollectionType type;

        public Collection(CollectionType type)
        {
            this.type = type;
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        public boolean isCollection()
        {
            return true;
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof Collection))
                return false;

            Collection that = (Collection)o;
            return type.equals(that.type);
        }

        @Override
        public final int hashCode()
        {
            return type.hashCode();
        }

        @Override
        public String toString()
        {
            boolean isFrozen = !this.type.isMultiCell();
            StringBuilder sb = new StringBuilder(isFrozen ? "frozen<" : "");
            switch (type.kind)
            {
                case LIST:
                    AbstractType<?> listType = ((ListType)type).getElementsType();
                    sb.append("list<").append(listType.asCQL3Type());
                    break;
                case SET:
                    AbstractType<?> setType = ((SetType)type).getElementsType();
                    sb.append("set<").append(setType.asCQL3Type());
                    break;
                case MAP:
                    AbstractType<?> keysType = ((MapType)type).getKeysType();
                    AbstractType<?> valuesType = ((MapType)type).getValuesType();
                    sb.append("map<").append(keysType.asCQL3Type()).append(", ").append(valuesType.asCQL3Type());
                    break;
                default:
                    throw new AssertionError();
            }
            sb.append(">");
            if (isFrozen)
                sb.append(">");
            return sb.toString();
        }
    }

    public static class UserDefined implements CQL3Type
    {
        // Keeping this separatly from type just to simplify toString()
        private final String name;
        private final UserType type;

        private UserDefined(String name, UserType type)
        {
            this.name = name;
            this.type = type;
        }

        public static UserDefined create(UserType type)
        {
            return new UserDefined(UTF8Type.instance.compose(type.name), type);
        }

        public boolean isCollection()
        {
            return false;
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof UserDefined))
                return false;

            UserDefined that = (UserDefined)o;
            return type.equals(that.type);
        }

        @Override
        public final int hashCode()
        {
            return type.hashCode();
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    public static class Tuple implements CQL3Type
    {
        private final TupleType type;

        private Tuple(TupleType type)
        {
            this.type = type;
        }

        public static Tuple create(TupleType type)
        {
            return new Tuple(type);
        }

        public boolean isCollection()
        {
            return false;
        }

        public AbstractType<?> getType()
        {
            return type;
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof Tuple))
                return false;

            Tuple that = (Tuple)o;
            return type.equals(that.type);
        }

        @Override
        public final int hashCode()
        {
            return type.hashCode();
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("tuple<");
            for (int i = 0; i < type.size(); i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(type.type(i).asCQL3Type());
            }
            sb.append(">");
            return sb.toString();
        }
    }
#endif

}
