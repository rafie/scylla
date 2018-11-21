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

#ifndef FEATURE_10
future<> data_listeners::uninstall_from_all_shards(distributed<database>& xdb, const utils::UUID& id) {
    return xdb.invoke_on_all([&id, this] (database& db) {
        db.data_listeners().uninstall(id);
    });
}
#endif // FEATURE_10

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
#ifdef DEBUG_TOPPARTITION
    dblog.debug("toppartitions_data_listener::on_read(id={}): {}.{}", _query_id.to_sstring(), s->ks_name(), s->cf_name());
#endif
    _top_k_read.append(to_hex(dk.key().representation()));
}

void toppartitions_data_listener::on_write(const schema_ptr& s, const frozen_mutation& m) {
#ifdef DEBUG_TOPPARTITION
    dblog.debug("toppartitions_data_listener::on_write(id={}): {}.{}", _id.to_sstring(), s->ks_name(), s->cf_name());
#endif
    auto pk = m.key(*s);
    _top_k_write.append(to_hex(pk.representation()));
}

std::unordered_map<toppartitions_query::query_id, lw_shared_ptr<toppartitions_query>> toppartitions_query::_queries;

toppartitions_query::toppartitions_query(distributed<database>& xdb, sstring ks, sstring cf, std::chrono::milliseconds duration)
        : _xdb(xdb), _ks(ks), _cf(cf) {
    _id = utils::UUID_gen::get_time_UUID();
    _duration = duration;
    dblog.info("toppartitions_query: id={}", _id.to_sstring());
}

future<> toppartitions_query::scatter() {
    return _xdb.invoke_on_all([&, this] (database& db) {
        db.data_listeners().install(std::make_unique<toppartitions_data_listener>(_id, _ks, _cf));
    });
}

using top_t = utils::space_saving_top_k<sstring>::results;

future<toppartitions_query::results> toppartitions_query::gather(unsigned res_size) {
    dblog.info("toppartitions_query::gather(id={}): {}.{}", id().to_sstring(), _ks, _cf);

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
                res.top_k_read.append(r.item, r.count);
            }
            for (auto& w: std::get<1>(rd_wr)) {
                res.top_k_write.append(w.item, w.count);
            }
            return std::move(res);
        });
}

future<toppartitions_query::results> toppartitions_query::run(distributed<database>& xdb, sstring ks,
        sstring cf, sstring duration) {
    try {
        std::chrono::milliseconds duration_ms{boost::lexical_cast<unsigned>(duration)};
        auto q = make_lw_shared<toppartitions_query>(xdb, ks, cf, duration_ms);
        _queries.try_emplace(q->id(), q);
        return q->scatter().then([q = std::move(q), &duration_ms] {
            return sleep(duration_ms).then([q = std::move(q)] () {
                return q->gather();
            });
        });
    } catch (boost::bad_lexical_cast&) {
        throw std::invalid_argument("duration should be numeric");
    }
}

#if 0
toppartitions_query::results::json_type toppartitions_query::results::map() const {
    auto rd = collect(top_k_read);
    auto wr = collect(top_k_write);

    return std::unordered_map<sstring, results_vec>{{"read", rd}, {"write", wr}};
}

toppartitions_query::results::results_vec
toppartitions_query::results::collect(const utils::space_saving_top_k<sstring>& data) const {
    std::vector<std::unordered_map<sstring, sstring>> v;
    for (auto& d: data.top(size)) {
        auto m = std::unordered_map<sstring, sstring>{
            {"partition", d.item},
            {"count", to_sstring(d.count)},
            {"error", to_sstring(d.error)}
        };
        v.emplace_back(m);
    }
    return v;
}
#endif // 0

#endif // FEATURE_3

} // namespace db
