/*
 * Copyright (C) 2017 ScyllaDB
 *
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

#include <unordered_map>
#include <iosfwd>
#include <experimental/string_view>
#ifndef FEATURE_1
#include <map>
#include <variant>
#endif // FEATURE_1

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>

#include "seastarx.hh"
#include "stdx.hh"

namespace seastar { class file; }
namespace YAML { class Node; }

namespace utils {

namespace bpo = boost::program_options;

#ifndef FEATURE_1
// Transforms:
//
// foo:
//   bar: 1
// list:
//   - first: a
//   - second: b
//
// Into ordered map:
//
// foo:bar: 1
// list:0:first: a
// list:1:second: b

class yaml_folded {
public:
    using yaml_map = std::map<sstring, std::optional<sstring>>;

protected:
    yaml_map _map;

    void parse(const YAML::Node& n, sstring key = "");

public:
    yaml_folded(const sstring& yaml_fname);
    yaml_folded(const YAML::Node& yaml_node);
    yaml_folded(const yaml_map& m) : _map(m) {}
    
    yaml_folded diff(const yaml_folded& old_cfg) const;

    yaml_map::const_iterator begin() const {
        return _map.begin();
    }

    yaml_map::iterator begin() {
        return _map.begin();
    }

    yaml_map::iterator end() {
        return _map.end();
    }

    yaml_map::const_iterator find(const sstring& key) const {
        return _map.find(key);
    }

    void print(std::ostream& out = std::cout) const;
};

#endif // FEATURE_1

class config_file {
public:
    typedef std::unordered_map<sstring, sstring> string_map;
    typedef std::vector<sstring> string_list;

    enum class value_status {
        Used,
        Unused,
        Invalid,
    };

    enum class config_source : uint8_t {
        None,
        SettingsFile,
        CommandLine
    };

    struct config_src {
        stdx::string_view _name, _desc;
    public:
        config_src(stdx::string_view name, stdx::string_view desc)
            : _name(name)
            , _desc(desc)
        {}
        virtual ~config_src() {}

        const stdx::string_view & name() const {
            return _name;
        }
        const stdx::string_view & desc() const {
            return _desc;
        }

        virtual void add_command_line_option(
                        bpo::options_description_easy_init&, const stdx::string_view&,
                        const stdx::string_view&) = 0;
        virtual void set_value(const YAML::Node&) = 0;
        virtual value_status status() const = 0;
        virtual config_source source() const = 0;
    };

    template<typename T, value_status S = value_status::Used>
    struct named_value : public config_src {
    private:
        friend class config;
        stdx::string_view _name, _desc;
        T _value = T();
        config_source _source = config_source::None;
    public:
        typedef T type;
        typedef named_value<T, S> MyType;

        named_value(stdx::string_view name, const T& t = T(), stdx::string_view desc = {})
            : config_src(name, desc)
            , _value(t)
        {}
        value_status status() const override {
            return S;
        }
        config_source source() const override {
            return _source;
        }
        bool is_set() const {
            return _source > config_source::None;
        }
        MyType & operator()(const T& t) {
            _value = t;
            return *this;
        }
        MyType & operator()(T&& t, config_source src = config_source::None) {
            _value = std::move(t);
            if (src > config_source::None) {
                _source = src;
            }
            return *this;
        }
        const T& operator()() const {
            return _value;
        }
        T& operator()() {
            return _value;
        }

        void add_command_line_option(bpo::options_description_easy_init&,
                        const stdx::string_view&, const stdx::string_view&) override;
        void set_value(const YAML::Node&) override;
    };

    typedef std::reference_wrapper<config_src> cfg_ref;

    config_file(std::initializer_list<cfg_ref> = {});

    void add(cfg_ref);
    void add(std::initializer_list<cfg_ref>);
    void add(const std::vector<cfg_ref> &);

    boost::program_options::options_description get_options_description();
    boost::program_options::options_description get_options_description(boost::program_options::options_description);

    boost::program_options::options_description_easy_init&
    add_options(boost::program_options::options_description_easy_init&);

    /**
     * Default behaviour for yaml parser is to throw on
     * unknown stuff, invalid opts or conversion errors.
     *
     * Error handling function allows overriding this.
     *
     * error: <option name>, <message>, <optional value_status>
     *
     * The last arg, opt value_status will tell you the type of
     * error occurred. If not set, the option found does not exist.
     * If invalid, it is invalid. Otherwise, a parse error.
     *
     */
    using error_handler = std::function<void(const sstring&, const sstring&, stdx::optional<value_status>)>;

    void read_from_yaml(const sstring&, error_handler = {});
    void read_from_yaml(const char *, error_handler = {});
    future<> read_from_file(const sstring&, error_handler = {});
    future<> read_from_file(file, error_handler = {});

    using configs = std::vector<cfg_ref>;

    configs set_values() const;
    configs unset_values() const;
    const configs& values() const {
        return _cfgs;
    }
private:
#ifndef FEATURE_1
    configs _cfgs;

    std::shared_ptr<yaml_folded> _folded;

public:
    std::shared_ptr<yaml_folded> folded() { return _folded; }
    void set_folded(std::shared_ptr<yaml_folded> folded) {
        _folded = std::move(folded);
    }
#else
    configs
        _cfgs;
#endif // FEATURE_1
};

extern template struct config_file::named_value<seastar::log_level, config_file::value_status::Used>;

}

