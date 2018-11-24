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

#include "db/data_listeners.hh"
#include "database.hh"
#include "db_clock.hh"

#ifndef FEATURE_3
#include "api/api-doc/column_family.json.hh"
#endif // FEATURE_3

#include <tuple>

extern logging::logger dblog;

namespace db {

#ifndef FEATURE_2

flat_mutation_reader partition_counting_listener::on_read(const schema_ptr& s, const dht::partition_range& range,
        const query::partition_slice& slice, flat_mutation_reader&& rd) {
    return make_filtering_reader(std::move(rd), [this, &range, &slice, s = std::move(s)] (const dht::decorated_key& dk) {
            this->on_read(s, range, slice, dk);
            return true;
        });
}

void data_listeners::install(std::unique_ptr<data_listener> listener) {
    dblog.debug("data_listeners: install id={}", listener->id().to_sstring());
    _listeners.push_back(std::move(listener));
}

void data_listeners::uninstall(const utils::UUID& id) {
    dblog.debug("data_listeners: uninstall id={}", id.to_sstring());
    auto it = _listeners.begin();
    while (it != _listeners.end()) {
        if ((*it)->id() == id) {
            it = _listeners.erase(it);
        } else {
            ++it;
        }
    }
}

flat_mutation_reader data_listeners::on_read(const schema_ptr& s, const dht::partition_range& range,
        const query::partition_slice& slice, flat_mutation_reader&& rd) {
    for (auto&& li : _listeners) {
        if (li->is_applicable(s)) {
            rd = li->on_read(s, range, slice, std::move(rd));
        }
    }
    return std::move(rd);
}

void data_listeners::on_write(const schema_ptr& s, const frozen_mutation& m) {
    for (auto&& li : _listeners) {
        if (li->is_applicable(s)) {
            li->on_write(s, m);
        }
    }
}

#endif // FEATURE_2

#ifndef FEATURE_3

void toppartitions_data_listener::on_read(const schema_ptr& s, const dht::partition_range& range,
        const query::partition_slice& slice, const dht::decorated_key& dk) {
    dblog.trace("toppartitions_data_listener::on_read: {}.{}", s->ks_name(), s->cf_name());

    _top_k_read.append(toppartitons_item_key{s, dk});
}

void toppartitions_data_listener::on_write(const schema_ptr& s, const frozen_mutation& m) {
    dblog.trace("toppartitions_data_listener::on_write: {}.{}", s->ks_name(), s->cf_name());

    _top_k_write.append(toppartitons_item_key{s, m.decorated_key(*s)});
}

toppartitions_query::toppartitions_query(distributed<database>& xdb, sstring ks, sstring cf,
    std::chrono::milliseconds duration, size_t list_size, size_t capacity)
        : _xdb(xdb), _ks(ks), _cf(cf), _duration(duration), _list_size(list_size), _capacity(capacity) {
    _id = utils::UUID_gen::get_time_UUID();
    dblog.info("toppartitions_query on {}.{}", _ks, _cf);
}

future<> toppartitions_query::scatter() {
    return _xdb.invoke_on_all([&, this] (database& db) {
        db.data_listeners().install(std::make_unique<toppartitions_data_listener>(_id, _ks, _cf));
    });
}

using top_t = toppartitions_data_listener::top_k::results;

future<toppartitions_query::results> toppartitions_query::gather(unsigned res_size) {
    return _xdb.map_reduce0(
        [res_size, this] (database& db) {
            for (auto& li: db.data_listeners().listeners()) {
                if (li->id() != _id) {
                    continue;
                }
                auto topp_li = dynamic_cast<toppartitions_data_listener*>(&*li);
                if (!topp_li) {
                    continue;
                }
                top_t rd = topp_li->_top_k_read.top(res_size);
                top_t wr = topp_li->_top_k_write.top(res_size);
                std::tuple<top_t, top_t> t{rd, wr};
                return std::move(t);
            }
            return std::move(std::tuple<top_t, top_t>());
        },
        results{res_size},
        [this] (results res, std::tuple<top_t, top_t> rd_wr) {

            for (auto& r: std::get<0>(rd_wr)) {
                res.read.append(r.item, r.count);
            }
            for (auto& w: std::get<1>(rd_wr)) {
                res.write.append(w.item, w.count);
            }
            return std::move(res);
        });
}

#endif // FEATURE_3

} // namespace db
