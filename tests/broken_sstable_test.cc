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

#include "tests/test-utils.hh"
#include "sstable_test.hh"

using namespace sstables;

namespace {
struct my_consumer {
    stop_iteration consume(static_row sr) { return stop_iteration::no; }
    stop_iteration consume(clustering_row cr) { return stop_iteration::no; }
    stop_iteration consume(range_tombstone rt) { return stop_iteration::no; }
    stop_iteration consume(tombstone tomb) { return stop_iteration::no; }
    void consume_end_of_stream() {}
    void consume_new_partition(const dht::decorated_key& dk) {}
    stop_iteration consume_end_of_partition() { return stop_iteration::no; }
};
}

static void broken_sst(sstring dir, unsigned long generation, schema_ptr s, sstring msg,
    sstable_version_types version = la) {
    try {
        sstable_ptr sstp = std::get<0>(reusable_sst(s, dir, generation, version).get());
        auto r = sstp->read_rows_flat(s);
        r.consume(my_consumer{}, db::no_timeout).get();
        BOOST_FAIL("expecting exception");
    } catch (malformed_sstable_exception& e) {
        BOOST_REQUIRE_EQUAL(sstring(e.what()), msg);
    }
}

static void broken_sst(sstring dir, unsigned long generation, sstring msg) {
    // Using an empty schema for this function, which is only about loading
    // a malformed component and checking that it fails.
    auto s = make_lw_shared(schema({}, "ks", "cf", {}, {}, {}, {}, utf8_type));
    return broken_sst(dir, generation, s, msg);
}

SEASTAR_THREAD_TEST_CASE(mismatched_timestamp) {
    schema_ptr s = schema_builder("test_ks", "test_table")
                       .with_column("key1", utf8_type, column_kind::partition_key)
                       .with_column("key2", utf8_type, column_kind::clustering_key)
                       .with_column("key3", utf8_type, column_kind::clustering_key)
                       .with_column("val", utf8_type, column_kind::regular_column)
                       .build(schema_builder::compact_storage::no);
    broken_sst("tests/sstables/mismatched_timestamp", 122, s,
        "Range tombstone with ck ckp{00056b65793262} and two different tombstones at ends: "
        "{tombstone: timestamp=1544745393692803, deletion_time=1544745393}, {tombstone: "
        "timestamp=1446576446577440, deletion_time=1442880998} in sstable "
        "tests/sstables/mismatched_timestamp/mc-122-big-Data.db",
        sstable::version_types::mc);
}

SEASTAR_THREAD_TEST_CASE(broken_open_tombstone) {
    schema_ptr s = schema_builder("test_ks", "test_table")
                       .with_column("key1", utf8_type, column_kind::partition_key)
                       .with_column("key2", utf8_type, column_kind::clustering_key)
                       .with_column("key3", utf8_type, column_kind::clustering_key)
                       .with_column("val", utf8_type, column_kind::regular_column)
                       .build(schema_builder::compact_storage::no);
    broken_sst("tests/sstables/broken_open_tombstone", 122, s,
        "Range tombstones have to be disjoint: current opened range tombstone { clustering: "
        "ckp{00056b65793262}, kind: incl start, tombstone: {tombstone: timestamp=1544745393692803, "
        "deletion_time=1544745393} }, new tombstone {tombstone: timestamp=1544745393692803, "
        "deletion_time=1544745393} in sstable "
        "tests/sstables/broken_open_tombstone/mc-122-big-Data.db",
        sstable::version_types::mc);
}

SEASTAR_THREAD_TEST_CASE(broken_close_tombstone) {
    schema_ptr s = schema_builder("test_ks", "test_table")
                       .with_column("key1", utf8_type, column_kind::partition_key)
                       .with_column("key2", utf8_type, column_kind::clustering_key)
                       .with_column("key3", utf8_type, column_kind::clustering_key)
                       .with_column("val", utf8_type, column_kind::regular_column)
                       .build(schema_builder::compact_storage::no);
    broken_sst("tests/sstables/broken_close_tombstone", 122, s,
        "Closing range tombstone that wasn't opened: clustering ckp{00056b65793262}, kind incl "
        "end, tombstone {tombstone: timestamp=1544745393692803, deletion_time=1544745393} in "
        "sstable tests/sstables/broken_close_tombstone/mc-122-big-Data.db",
        sstable::version_types::mc);
}

SEASTAR_THREAD_TEST_CASE(broken_start_composite) {
    schema_ptr s =
        schema_builder("test_ks", "test_table")
            .with_column("test_key", utf8_type, column_kind::partition_key)
            .with_column("test_val", utf8_type, column_kind::clustering_key)
            .build(schema_builder::compact_storage::no);
    broken_sst("tests/sstables/broken_start_composite", 76, s,
        "Unexpected start composite marker 2 in sstable tests/sstables/broken_start_composite/la-76-big-Data.db");
}

SEASTAR_THREAD_TEST_CASE(broken_end_composite) {
    schema_ptr s =
        schema_builder("test_ks", "test_table")
            .with_column("test_key", utf8_type, column_kind::partition_key)
            .with_column("test_val", utf8_type, column_kind::clustering_key)
            .build(schema_builder::compact_storage::no);
    broken_sst("tests/sstables/broken_end_composite", 76, s,
        "Unexpected end composite marker 3 in sstable tests/sstables/broken_end_composite/la-76-big-Data.db");
}

SEASTAR_THREAD_TEST_CASE(static_mismatch) {
    schema_ptr s =
        schema_builder("test_foo_bar_zed_baz_ks", "test_foo_bar_zed_baz_table")
            .with_column("test_foo_bar_zed_baz_key", utf8_type, column_kind::partition_key)
            .with_column("test_foo_bar_zed_baz_val", utf8_type, column_kind::clustering_key)
            .with_column("test_foo_bar_zed_baz_static", utf8_type, column_kind::regular_column)
            .build(schema_builder::compact_storage::no);
    broken_sst("tests/sstables/static_column", 58, s,
        "Mismatch between static cell and non-static column definition in sstable "
        "tests/sstables/static_column/la-58-big-Data.db");
}

SEASTAR_THREAD_TEST_CASE(static_with_clustering) {
    schema_ptr s =
        schema_builder("test_foo_bar_zed_baz_ks", "test_foo_bar_zed_baz_table")
            .with_column("test_foo_bar_zed_baz_key", utf8_type, column_kind::partition_key)
            .with_column("test_foo_bar_zed_baz_val", utf8_type, column_kind::clustering_key)
            .with_column("test_foo_bar_zed_baz_static", utf8_type, column_kind::static_column)
            .build(schema_builder::compact_storage::no);
    broken_sst("tests/sstables/static_with_clustering", 58, s,
        "Static row has clustering key information. I didn't expect that! in sstable "
        "tests/sstables/static_with_clustering/la-58-big-Data.db");
}

SEASTAR_THREAD_TEST_CASE(zero_sized_histogram) {
    broken_sst("tests/sstables/zero_sized_histogram", 5,
               "Estimated histogram with zero size found. Can't continue! in sstable "
               "tests/sstables/zero_sized_histogram/la-5-big-Statistics.db");
}

SEASTAR_THREAD_TEST_CASE(bad_column_name) {
    broken_sst("tests/sstables/bad_column_name", 58,
               "Found 3 clustering elements in column name. Was not expecting that! in sstable "
               "tests/sstables/bad_column_name/la-58-big-Data.db");
}

SEASTAR_THREAD_TEST_CASE(empty_toc) {
    broken_sst("tests/sstables/badtoc", 1,
               "Empty TOC in sstable tests/sstables/badtoc/la-1-big-TOC.txt");
}

SEASTAR_THREAD_TEST_CASE(alien_toc) {
    broken_sst("tests/sstables/badtoc", 2,
               "tests/sstables/badtoc/la-2-big-Statistics.db: file not found");
}

SEASTAR_THREAD_TEST_CASE(truncated_toc) {
    broken_sst("tests/sstables/badtoc", 3,
               "tests/sstables/badtoc/la-3-big-Statistics.db: file not found");
}

SEASTAR_THREAD_TEST_CASE(wrong_format_toc) {
    broken_sst("tests/sstables/badtoc", 4,
               "tests/sstables/badtoc/la-4-big-TOC.txt: file not found");
}

SEASTAR_THREAD_TEST_CASE(compression_truncated) {
    broken_sst("tests/sstables/badcompression", 1,
               "tests/sstables/badcompression/la-1-big-Statistics.db: file not found");
}

SEASTAR_THREAD_TEST_CASE(compression_bytes_flipped) {
    broken_sst("tests/sstables/badcompression", 2,
               "tests/sstables/badcompression/la-2-big-Statistics.db: file not found");
}
