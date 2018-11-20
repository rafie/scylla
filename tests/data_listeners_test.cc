/*
 * Copyright (C) 2018 ScyllaDB
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

#include <boost/test/unit_test.hpp>

#include "tests/test-utils.hh"
#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"
#include "cql3/query_processor.hh"

#include "db/data_listeners.hh"

using namespace std;
using namespace std::chrono_literals;

logging::logger testlog("test"); 

class table_listener : public db::partition_counting_listener {
    sstring _cf_name;

public:
    table_listener(sstring cf_name, const utils::UUID& id) : db::partition_counting_listener(id), 
        _cf_name(cf_name) {}

    virtual void on_read(const schema_ptr& s, const dht::partition_range& range, 
            const query::partition_slice& slice, const dht::decorated_key& dk) override {
        if (s->cf_name() == _cf_name) {
            ++read;
        }
    }

    virtual void on_write(const schema_ptr& s, const frozen_mutation& m) override { 
        if (s->cf_name() == _cf_name) {
            ++write;
        }
    }

    unsigned read = 0;
    unsigned write = 0;
};

struct results {
    unsigned read = 0;
    unsigned write = 0;
};

//---------------------------------------------------------------------------------------------

results test_data_listeners(cql_test_env& e, sstring cf_name) {
    testlog.info("starting test_data_listeners");

    auto id = table_listener::make_id();
    
    e.db().invoke_on_all([&id, &cf_name] (database& db) {
        db.data_listeners().install(std::make_unique<table_listener>(cf_name, id));
    }).get();
    
    e.execute_cql("CREATE TABLE t1 (k int, c int, PRIMARY KEY (k, c));").get();
    e.execute_cql("INSERT INTO t1 (k, c) VALUES (1, 1);").get();
    e.execute_cql("INSERT INTO t1 (k, c) VALUES (2, 2);").get();
    e.execute_cql("INSERT INTO t1 (k, c) VALUES (3, 3);").get();
    e.execute_cql("SELECT k, c FROM t1;").get();

    auto res = e.db().map_reduce0(
        [] (database& db) {
            for (auto& li: db.data_listeners().listeners()) {
                table_listener* t1_li = dynamic_cast<table_listener*>(&*li);
                if (t1_li) {
                    results res{t1_li->read, t1_li->write};
                    return std::move(res);
                }
            }
            return std::move(results{});
        }, 
        results{},
        [] (results res, results li_res) {
            res.read += li_res.read;
            res.write += li_res.write;
            return std::move(res);
        }).get0();

    testlog.info("test_data_listeners: read={} write={}", res.read, res.write);

    e.db().invoke_on_all([&id] (database& db) {
        db.data_listeners().uninstall(id);
    }).get();

    return res;
}

SEASTAR_TEST_CASE(test_dlistener_t1) {
    return do_with_cql_env_thread([] (auto& e) {
        auto res = test_data_listeners(e, "t1");
        BOOST_REQUIRE_EQUAL(3, res.read);
        BOOST_REQUIRE_EQUAL(3, res.write);
    });
} 

SEASTAR_TEST_CASE(test_dlistener_t2) {
    return do_with_cql_env_thread([] (auto& e) {
        auto res = test_data_listeners(e, "t2");
        BOOST_REQUIRE_EQUAL(0, res.read);
        BOOST_REQUIRE_EQUAL(0, res.write);
    });
} 
