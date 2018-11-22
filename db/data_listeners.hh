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

#pragma once

#include "seastar/core/distributed.hh"
#include "seastar/core/future.hh"
#include "seastar/core/distributed.hh"

#include "schema.hh"
#include "flat_mutation_reader.hh"
#include "mutation_reader.hh"
#include "frozen_mutation.hh"
#include "utils/top_k.hh"
#include "json/json_elements.hh"
#include "api/api-doc/column_family.json.hh"

#include <vector>
#include <unordered_map>

#ifndef FEATURE_2

namespace db {

class data_listener {
protected:
    utils::UUID _id;

public:
    data_listener(const utils::UUID& id) : _id(id) {}

    // Invoked for each write, with partition granularity.
    // The schema_ptr passed is the one which corresponds to the incoming mutation, not the current schema of the table.
    virtual void on_write(const schema_ptr&, const frozen_mutation&) { }

    // Invoked for each query (both data query and mutation query) when a mutation reader is created.
    // Paging queries may invoke this once for a page, or less often, depending on whether they hit in the querier cache or not.
    //
    // The mutation_reader passed to this method is the reader from which the query results are built (uncompacted).
    // This method replaces that reader with the one returned from this method.
    // This allows the listener to install on-the-fly processing for the mutation stream.
    //
    // The schema_ptr passed is the one which corresponds to the reader, not the current schema of the table.
    virtual flat_mutation_reader on_read(const schema_ptr& s, const dht::partition_range& range,
            const query::partition_slice& slice, flat_mutation_reader&& rd) {
        return std::move(rd);
    }

    const utils::UUID& id() const { return _id; }
    virtual bool is_applicable(const schema_ptr& s) const { return true; }

    static utils::UUID make_id() { return utils::UUID_gen::get_time_UUID(); }
};

class partition_counting_listener : public data_listener {
public:
    partition_counting_listener(const utils::UUID& id) : data_listener(id) {}

    virtual flat_mutation_reader on_read(const schema_ptr& s, const dht::partition_range& range,
        const query::partition_slice& slice, flat_mutation_reader&& rd) override;

    virtual void on_read(const schema_ptr& s, const dht::partition_range& range,
        const query::partition_slice& slice, const dht::decorated_key& dk) {}

    const utils::UUID& id() const { return _id; }
};

class data_listeners /*: public data_listener */ {
    using listeners_list = std::vector<std::unique_ptr<data_listener>>;

private:
    database& _db;
    listeners_list _listeners;

public:
    data_listeners(database& db) : _db(db) {}

    void install(std::unique_ptr<data_listener> listener);
    void uninstall(const utils::UUID& id);

    virtual flat_mutation_reader on_read(const schema_ptr& s, const dht::partition_range& range,
            const query::partition_slice& slice, flat_mutation_reader&& rd);
    virtual void on_write(const schema_ptr& s, const frozen_mutation& m);

    listeners_list& listeners() { return _listeners; }

    bool empty() const { return _listeners.empty(); }
};

#endif // FEATURE_2

#ifndef FEATURE_3

class toppartitions_data_listener : public partition_counting_listener {
    friend class toppartitions_query;

    sstring _ks;
    sstring _cf;
    utils::space_saving_top_k<sstring> _top_k_read;
    utils::space_saving_top_k<sstring> _top_k_write;

    virtual bool is_applicable(const schema_ptr& s) const override {
        return s->ks_name() == _ks && s->cf_name() == _cf;
    }

public:
    toppartitions_data_listener(const utils::UUID& query_id, sstring ks, sstring cf)
        : partition_counting_listener(query_id), _ks(ks), _cf(cf) {}

    virtual void on_read(const schema_ptr& s, const dht::partition_range& range,
        const query::partition_slice& slice, const dht::decorated_key& dk) override;

    virtual void on_write(const schema_ptr& s, const frozen_mutation& m) override;
};

class toppartitions_query {
public:
    using query_id = utils::UUID;

private:
    distributed<database>& _xdb;
    query_id _id;
    sstring _ks;
    sstring _cf;
    std::chrono::milliseconds _duration;
    size_t _list_size;
    size_t _capacity;

public:
    toppartitions_query(seastar::distributed<database>& xdb, sstring ks, sstring cf,
        std::chrono::milliseconds duration, size_t list_size, size_t capacity);

    struct results {
        utils::space_saving_top_k<sstring> read;
        utils::space_saving_top_k<sstring> write;

        results(size_t capacity) : read(capacity), write(capacity) {}
    };

    std::chrono::milliseconds duration() const { return _duration; }
    size_t list_size() const { return _list_size; }
    size_t capacity() const { return _capacity; }

public:
    future<> scatter();
    future<results> gather(unsigned results_size = 256);
};

#endif // FEATURE_3

} // namespace db
