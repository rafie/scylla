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

#include <unordered_map>
#include <regex>

#include <yaml-cpp/yaml.h>

#include <boost/program_options.hpp>
#include <boost/any.hpp>
#include <boost/range/adaptor/filtered.hpp>

#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/print.hh>
#ifndef FEATURE_4
#include <seastar/util/program-options.hh>
#endif // FEATURE_4

#include "log.hh"

#include "config_file.hh"
#include "config_file_impl.hh"

logging::logger configlog("config"); 

namespace bpo = boost::program_options;

template<>
std::istream& std::operator>>(std::istream& is, std::unordered_map<seastar::sstring, seastar::sstring>& map) {
   std::istreambuf_iterator<char> i(is), e;

   int level = 0;
   bool sq = false, dq = false, qq = false;
   sstring key, val;
   sstring* ps = &key;

   auto add = [&] {
      if (!key.empty()) {
         map[key] = std::move(val);
      }
      key = {};
      val = {};
      ps = &key;
   };

   while (i != e && level >= 0) {
      auto c = *i++;

      switch (c) {
      case '\\':
         qq = !qq;
         if (qq) {
            continue;
         }
         break;
      case '\'':
         if (!qq) {
            sq = !sq;
         }
         break;
      case '"':
         if (!qq) {
            dq = !dq;
         }
         break;
      case '=':
         if (level <= 1 && !sq && !dq && !qq) {
            ps = &val;
            continue;
         }
         break;
      case '{': case '[':
         if (!sq && !dq && !qq) {
            ++level;
            continue;
         }
         break;
      case ']': case '}':
         if (!sq && !dq && !qq && level > 0) {
            --level;
            continue;
         }
         break;
      case ',':
         if (level == 1 && !sq && !dq && !qq) {
            add();
            continue;
         }
         break;
      case ' ': case '\t': case '\n':
         if (!sq && !dq && !qq) {
            continue;
         }
         break;
      default:
         break;
      }

      if (level == 0) {
         ++level;
      }
      qq = false;
      ps->append(&c, 1);
   }

   add();

   return is;
}

template<>
std::istream& std::operator>>(std::istream& is, std::vector<seastar::sstring>& res) {
   std::istreambuf_iterator<char> i(is), e;

   int level = 0;
   bool sq = false, dq = false, qq = false;
   sstring val;

   auto add = [&] {
      if (!val.empty()) {
         res.emplace_back(std::exchange(val, {}));
      }
      val = {};
   };

   while (i != e && level >= 0) {
      auto c = *i++;

      switch (c) {
      case '\\':
         qq = !qq;
         if (qq) {
            continue;
         }
         break;
      case '\'':
         if (!qq) {
            sq = !sq;
         }
         break;
      case '"':
         if (!qq) {
            dq = !dq;
         }
         break;
      case '{': case '[':
         if (!sq && !dq && !qq) {
            ++level;
            continue;
         }
         break;
      case '}': case ']':
         if (!sq && !dq && !qq && level > 0) {
            --level;
            continue;
         }
         break;
      case ',':
         if (level == 1 && !sq && !dq && !qq) {
            add();
            continue;
         }
         break;
      case ' ': case '\t': case '\n':
         if (!sq && !dq && !qq) {
            continue;
         }
         break;
      default:
         break;
      }

      if (level == 0) {
         ++level;
      }
      qq = false;
      val.append(&c, 1);
   }

   add();

   return is;
}

template std::istream& std::operator>>(std::istream&, std::unordered_map<seastar::sstring, seastar::sstring>&);

namespace utils {

sstring hyphenate(const stdx::string_view& v) {
    sstring result(v.begin(), v.end());
    std::replace(result.begin(), result.end(), '_', '-');
    return result;
}

sstring dehyphenate(const stdx::string_view& v) {
    sstring result(v.begin(), v.end());
    std::replace(result.begin(), result.end(), '-', '_');
    return result;
}

config_file::config_file(std::initializer_list<cfg_ref> cfgs) : _cfgs(cfgs)
{}

void config_file::add(cfg_ref cfg) {
    _cfgs.emplace_back(cfg);
}

void config_file::add(std::initializer_list<cfg_ref> cfgs) {
    _cfgs.insert(_cfgs.end(), cfgs.begin(), cfgs.end());
}

void config_file::add(const std::vector<cfg_ref> & cfgs) {
    _cfgs.insert(_cfgs.end(), cfgs.begin(), cfgs.end());
}

bpo::options_description config_file::get_options_description() {
    bpo::options_description opts("");
    return get_options_description(opts);
}

bpo::options_description config_file::get_options_description(boost::program_options::options_description opts) {
    auto init = opts.add_options();
    add_options(init);
    return std::move(opts);
}

bpo::options_description_easy_init&
config_file::add_options(bpo::options_description_easy_init& init) {
    for (config_src& src : _cfgs) {
        if (src.status() == value_status::Used) {
            auto&& name = src.name();
            sstring tmp(name.begin(), name.end());
            std::replace(tmp.begin(), tmp.end(), '_', '-');
            src.add_command_line_option(init, tmp, src.desc());
        }
    }
    return init;
}

void config_file::read_from_yaml(const sstring& yaml, error_handler h) {
    read_from_yaml(yaml.c_str(), std::move(h));
}

void config_file::read_from_yaml(const char* yaml, error_handler h) {
    std::unordered_map<sstring, cfg_ref> values;

    if (!h) {
        h = [](auto & opt, auto & msg, auto) {
            throw std::invalid_argument(msg + " : " + opt);
        };
    }
    /*
     * Note: this is not very "half-fault" tolerant. I.e. there could be
     * yaml syntax errors that origin handles and still sets the options
     * where as we don't...
     * There are no exhaustive attempts at converting, we rely on syntax of
     * file mapping to the data type...
     */
    auto doc = YAML::Load(yaml);
    for (auto node : doc) {
        auto label = node.first.as<sstring>();

        auto i = std::find_if(_cfgs.begin(), _cfgs.end(), [&label](const config_src& cfg) { return cfg.name() == label; });
        if (i == _cfgs.end()) {
            h(label, "Unknown option", stdx::nullopt);
            continue;
        }

        config_src& cfg = *i;

        if (cfg.source() > config_source::SettingsFile) {
            // already set
            continue;
        }
        switch (cfg.status()) {
        case value_status::Invalid:
            h(label, "Option is not applicable", cfg.status());
            continue;
        case value_status::Unused:
        default:
            break;
        }
        if (node.second.IsNull()) {
            continue;
        }
        // Still, a syntax error is an error warning, not a fail
        try {
            cfg.set_value(node.second);
        } catch (std::exception& e) {
            h(label, e.what(), cfg.status());
        } catch (...) {
            h(label, "Could not convert value", cfg.status());
        }
    }
}

config_file::configs config_file::set_values() const {
    return boost::copy_range<configs>(_cfgs | boost::adaptors::filtered([] (const config_src& cfg) {
        return cfg.status() > value_status::Used || cfg.source() > config_source::None;
    }));
}

config_file::configs config_file::unset_values() const {
    configs res;
    for (config_src& cfg : _cfgs) {
        if (cfg.status() > value_status::Used) {
            continue;
        }
        if (cfg.source() > config_source::None) {
            continue;
        }
        res.emplace_back(cfg);
    }
    return res;
}

future<> config_file::read_from_file(file f, error_handler h) {
    return f.size().then([this, f, h](size_t s) {
        return do_with(make_file_input_stream(f), [this, s, h](input_stream<char>& in) {
            return in.read_exactly(s).then([this, h](temporary_buffer<char> buf) {
               read_from_yaml(sstring(buf.begin(), buf.end()), h);
            });
        });
    });
}

future<> config_file::read_from_file(const sstring& filename, error_handler h) {
    return open_file_dma(filename, open_flags::ro).then([this, h](file f) {
       return read_from_file(std::move(f), h);
    });
}

stdx::optional<config_file::cfg_ref> config_file::find(sstring name) {
    for (auto& ci: values()) {
        auto& c = ci.get();
        if (c.name() == name) {
            return ci;
        }
    }
    return stdx::nullopt;
}

void config_file::print(sstring title, std::ostream& out) const {
    if (!title.empty()) {
        out << title << ":\n";
    }
    out << _cfgs;
}

std::ostream& operator<<(std::ostream& out, const config_file::configs& cfg) {
    for (auto& ci: cfg) {
        auto& c = ci.get();
        sstring source;
        switch (c.source()) {
        case config_file::config_source::None: 
            source = "none"; 
            continue;
        case config_file::config_source::SettingsFile: 
            source = "yaml"; 
            break;
        case config_file::config_source::CommandLine: 
            source = "cmdline"; 
            break;
        };
        out << "> " << c.name() << ": " << source << ": " << c.text_value() << "\n";
    }
    out << "---\n";
    return out;
}

#ifndef FEATURE_2
config_file::configs config_file::diff(const config_file &old_file) const {
    std::map<stdx::string_view, cfg_ref> old_cfg, new_cfg;
    config_file::configs diff;

    for (auto& ci: values()) {
        auto& c = ci.get();
        if (c.source() == config_source::SettingsFile) {
            new_cfg.try_emplace(c.name(), ci);
        }
    }

    for (auto& ci: old_file.values()) {
        auto& c = ci.get();
        if (c.source() == config_source::SettingsFile) {
            old_cfg.try_emplace(c.name(), ci);
        }
    }

    auto new_i = new_cfg.begin();
    auto old_i = old_cfg.begin();

    while (new_i != new_cfg.end() && old_i != old_cfg.end()) {
        
        if (new_i->first < old_i->first) { // added
            diff.emplace_back(new_i->second);
            ++new_i;
        } else if (old_i->first < new_i->first) { // removed
            // nop
            ++old_i;
        } else { // changed
            if (new_i->second.get().text_value() != old_i->second.get().text_value()) {
                diff.emplace_back(new_i->second);
            }
            ++new_i, ++old_i;
        }
    }

    // added
    for (; new_i != new_cfg.end(); ++new_i) {
        diff.emplace_back(new_i->second);
    }

    // items removed - nop

    return diff;
}
#endif // FEATURE_2

// boost::any to be re-assigned to its original type

template <class T>
void any_compat_set(boost::any& a, T&& b) {
    auto& t = a.type();    
    if (t == typeid(std::string)) {
        a = boost::lexical_cast<std::string>(b);
    } else if (t == typeid(seastar::sstring)) {
        a = boost::lexical_cast<sstring>(b);
    } else if (t == typeid(int)) {
        a = boost::lexical_cast<int>(b);
    } else if (t == typeid(int32_t)) {
        a = boost::lexical_cast<int32_t>(b);
    } else if (t == typeid(uint32_t)) {
        a = boost::lexical_cast<uint32_t>(b);
    } else if (t == typeid(int64_t)) {
        a = boost::lexical_cast<int64_t>(b);
    } else if (t == typeid(unsigned)) {
        a = boost::lexical_cast<unsigned>(b);
    } else if (t == typeid(bool)) {
        a = boost::lexical_cast<bool>(b);
    } else if (t == typeid(float)) {
        a = boost::lexical_cast<float>(b);
    } else if (t == typeid(double)) {
        a = boost::lexical_cast<double>(b);
    } else {
        throw boost::bad_lexical_cast(typeid(b), t);
    }
}

static sstring
to_sstring(const bpo::variable_value& v) {
    boost::any a = v.value();
    auto& t = a.type();
    
    sstring s;
    if (t == typeid(std::string)) {
        s = v.as<std::string>();
    } else if (t == typeid(seastar::sstring)) {
        s = v.as<sstring>();
    } else if (t == typeid(int)) {
        s = boost::lexical_cast<std::string>(v.as<int>());
    } else if (t == typeid(int32_t)) {
        s = boost::lexical_cast<std::string>(v.as<int32_t>());
    } else if (t == typeid(uint32_t)) {
        s = boost::lexical_cast<std::string>(v.as<uint32_t>());
    } else if (t == typeid(int64_t)) {
        s = boost::lexical_cast<std::string>(v.as<int64_t>());
    } else if (t == typeid(unsigned)) {
        s = boost::lexical_cast<std::string>(v.as<unsigned>());
    } else if (t == typeid(bool)) {
        s = boost::lexical_cast<std::string>(v.as<bool>());
    } else if (t == typeid(float)) {
        s = boost::lexical_cast<std::string>(v.as<float>());
    } else if (t == typeid(double)) {
        s = boost::lexical_cast<std::string>(v.as<double>());
    } else {
        throw boost::bad_lexical_cast(t, typeid(sstring));
    }
    
    return s;
}

void config_file::sync(bpo::variables_map& opts) {
    // opts->config: opts should override existing config
    sstring item_name;
    
    for (auto opt_i: opts) {
        try {
            auto &opt_name = opt_i.first;
            auto &opt = opt_i.second;

            item_name = opt_name;

            if (opt.empty() || opt.defaulted()) {
                continue;
            }

            auto cfg_name{dehyphenate(opt_name)};
            stdx::optional<cfg_ref> cfg_item = find(cfg_name);
            if (!cfg_item) {
                continue;
            }
            sstring opt_sval;
            try {
                opt_sval = to_sstring(opt);
            } catch (...) {
                continue;
            }
            
            auto cfg_sval = cfg_item->get().text_value();
            if (cfg_sval == opt_sval) {
                continue;
            }
            configlog.info("sync opts->yaml: {}={} [was {}]", opt_name, opt_sval, cfg_sval);
            cfg_item->get().set_value(opt_sval);
        } catch (...) {
            configlog.error("sync opt->yaml: problem with {}", item_name);
        }
    }

    // config->opts: set if config is present in file (not by default) and opt is missing
    for (auto& cfg_i: values()) {
        try {
            auto& cfg = cfg_i.get();

            sstring cfg_name{cfg.name()};
            auto opt_name = hyphenate(cfg_name);
            item_name = opt_name;

            auto& opt = opts[opt_name];
            if (! (opt.empty() || opt.defaulted())) {
                continue;
            }

            sstring opt_sval;
            try {
                opt_sval = to_sstring(opt);
            } catch (...) {}
          
            if (cfg.source() == config_source::SettingsFile && cfg.status() == value_status::UsedFromSeastar) {
                auto cfg_sval = cfg.text_value();
                if (cfg_sval != opt_sval) {
                    configlog.info("sync yaml->opts: {}={} [was {}]", cfg_name, cfg_sval, opt_sval);
                    try {
                        any_compat_set(opts.at(std::string(opt_name)).value(), cfg_sval);
                    } catch (...) {
                        opts.insert(std::make_pair(opt_name, bpo::variable_value(cfg.value(), false)));
                    }
                }
            }
        } catch (...) {
            configlog.error("sync yaml->opt: problem with {}", item_name);
        }
    }
}

#ifndef FEATURE_4

std::ostream& operator<<(std::ostream& out, logger_timestamp_style lts) {
    switch (lts) {
    case logger_timestamp_style::none: return out << "none";
    case logger_timestamp_style::boot: return out << "boot";
    case logger_timestamp_style::real: return out << "real";
    }
    return out;
}

void print(const bpo::variables_map& vm, sstring title, std::ostream& out)
{
    if (!title.empty()) {
        out << title << "\n";
    }
    for (auto it: vm)
    {
        auto &name = it.first;
        auto &v = it.second;
        boost::any a = v.value();
    
        out << "> " << name;
        if (v.empty() || a.empty()) {
            out << "(empty)";
        }
        if (v.defaulted()) {
            out << "(default)";
        }
        out << "=";

        try {
            auto s = utils::to_sstring(v);
            out << s;
        } catch (...) {
            auto& t = a.type(); 
            if (t == typeid(seastar::program_options::string_map)) {
                out << "[";
                for (auto j: v.as<seastar::program_options::string_map>())
                    out << j.first << ":" << j.second << " ";
                out << "]";
            }
            else if (t == typeid(seastar::logger_timestamp_style)) {
                out << v.as<seastar::logger_timestamp_style>();
            }
            else {
                try {
                    std::vector<std::string> w = v.as<std::vector<std::string>>();
                    uint i = 0;
                    for (auto j: w)
                        out << "\r> " << name << "[" << i++ << "]=" << j << "\n";
                }
                catch (...) {
                    out << "UnknownType(" << t.name() << ")";
                }
            }
        }
        out << "\n";
    }
    out << "---\n";
}

#endif // FEATURE_4

} // namespace utils
