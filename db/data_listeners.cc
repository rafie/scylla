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

#include <tuple>

extern logging::logger dblog;

namespace db {

#ifndef FEATURE_2

void data_listeners::install(data_listener* listener) {
    _listeners.emplace(listener, 0);
    dblog.debug("data_listeners: install listener {}", listener);
}

void data_listeners::uninstall(data_listener* listener) {
    dblog.debug("data_listeners: uninstall listener {}", listener);
    _listeners.extract(listener);
}

bool data_listeners::exists(data_listener* listener) const {
    return _listeners.find(listener) != _listeners.end();
}

flat_mutation_reader data_listeners::on_read(const schema_ptr& s, const dht::partition_range& range,
        const query::partition_slice& slice, flat_mutation_reader&& rd) {
    for (auto&& li : _listeners) {
        rd = li.first->on_read(s, range, slice, std::move(rd));
    }
    return std::move(rd);
}

void data_listeners::on_write(const schema_ptr& s, const frozen_mutation& m) {
    for (auto&& li : _listeners) {
        li.first->on_write(s, m);
    }
}

#endif // FEATURE_2

#ifndef FEATURE_3

flat_mutation_reader toppartitions_data_listener::on_read(const schema_ptr& s, const dht::partition_range& range,
        const query::partition_slice& slice, flat_mutation_reader&& rd) {
    if (s->ks_name() != _ks || s->cf_name() != _cf) {
        return std::move(rd);
    }
    dblog.trace("toppartitions_data_listener::on_read: {}.{}", s->ks_name(), s->cf_name());
    return make_filtering_reader(std::move(rd), [this, &range, &slice, s = std::move(s)] (const dht::decorated_key& dk) {
            _top_k_read.append(toppartitons_item_key{s, dk});
            return true;
        });
}

void toppartitions_data_listener::on_write(const schema_ptr& s, const frozen_mutation& m) {
    if (s->ks_name() != _ks || s->cf_name() != _cf) {
        return;
    }
    dblog.trace("toppartitions_data_listener::on_write: {}.{}", s->ks_name(), s->cf_name());
    _top_k_write.append(toppartitons_item_key{s, m.decorated_key(*s)});
}

toppartitions_query::toppartitions_query(distributed<database>& xdb, sstring ks, sstring cf,
    std::chrono::milliseconds duration, size_t list_size, size_t capacity)
        : _xdb(xdb), _ks(ks), _cf(cf), _duration(duration), _list_size(list_size), _capacity(capacity) {
    dblog.info("toppartitions_query on {}.{}", _ks, _cf);
}

future<> toppartitions_query::scatter() {
    return _xdb.map_reduce0(
        [this] (database& db) {
            auto listener = std::make_unique<toppartitions_data_listener>(_ks, _cf);
            db.data_listeners().install(&*listener);
            return std::move(listener);
        },
        std::vector<std::unique_ptr<toppartitions_data_listener>>{},
        [this] (auto&& listeners, auto&& listener) {
            listeners.push_back(std::move(listener));
            return std::move(listeners);
        }).then([this](auto&& listeners) {
            _listeners = std::move(listeners);
            return make_ready_future<>();
        });

}

using top_t = toppartitions_data_listener::top_k::results;

future<toppartitions_query::results> toppartitions_query::gather(unsigned res_size) {
    return _xdb.map_reduce0(
        [res_size, this] (database& db) {
            for (auto& li: _listeners) {
                if (!db.data_listeners().exists(&*li)) {
                    continue;
                }

                top_t rd = li->_top_k_read.top(res_size);
                top_t wr = li->_top_k_write.top(res_size);

                db.data_listeners().uninstall(&*li);

                std::tuple<top_t, top_t> t{rd, wr};
                return std::move(t);
            }
            return std::move(std::tuple<top_t, top_t>());
        },
        results{res_size},
        [this] (results res, std::tuple<top_t, top_t> rd_wr) {
            res.read.append(std::get<0>(rd_wr));
            res.write.append(std::get<1>(rd_wr));
            return std::move(res);
        });
}

#endif // FEATURE_3

} // namespace db
