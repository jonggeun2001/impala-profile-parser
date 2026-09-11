import base64
import zlib

import pytest

from impala_profile_parser.decoder import ProfileError, decode_line, normalize_query_id
from impala_profile_parser.vendor.RuntimeProfile.ttypes import (
    TAggCounter,
    TAggEventSequence,
    TAggSummaryStatsCounter,
    TAggTimeSeriesCounter,
    TAggregatedRuntimeProfileNode,
    TCounter,
    TEventSequence,
)
from tests.helpers import encode_profile_line, encode_raw, make_node, make_tree, serialize_tree


V1_LINE = (
    b"1234 1:2 "
    b"eJyT1JFgDSxNLaoUZZLkEWMUkmbq4ADzFTxdFA1QgaEVmoARb3BhjkJwSWJJam5qXglHcWpOanKJgqGkBtwMVBXSDAwSrM4ZmTkpogxA6/gEpRkkOYCCDAB1TR+X"
)


def test_decodes_fixed_impala_v1_fixture():
    profile = decode_line(V1_LINE)

    assert profile.query_id == "0000000000000001:0000000000000002"
    assert profile.logged_at_ms == 1234
    assert profile.tree.profile_version == 1
    assert [node.name for node in profile.tree.nodes] == ["Query", "Child"]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("1:2", "0000000000000001:0000000000000002"),
        ("ABCDEF:000F", "0000000000abcdef:000000000000000f"),
        ("ffffffffffffffff:FFFFFFFFFFFFFFFF", "ffffffffffffffff:ffffffffffffffff"),
    ],
)
def test_normalizes_query_id(value, expected):
    assert normalize_query_id(value) == expected


@pytest.mark.parametrize(
    "line",
    [
        b"",
        b"1234 1:2",
        b"1234 1:2 payload extra",
        b"1234\t1:2\tpayload\textra",
    ],
)
def test_rejects_non_three_token_envelopes(line):
    assert_error_code(line, "invalid_envelope")


@pytest.mark.parametrize("timestamp", [b"-1", b"+1", b"1.0", b"9223372036854775808"])
def test_rejects_invalid_int64_timestamp(timestamp):
    line = timestamp + b" 1:2 eJwDAAAAAAE="

    assert_error_code(line, "invalid_timestamp")


@pytest.mark.parametrize(
    "query_id",
    ["", "1", "1:2:3", "0x1:2", "1234567890abcdef0:2", "gg:2", " 1:2"],
)
def test_rejects_malformed_query_id_without_echoing_it(query_id):
    with pytest.raises(ProfileError) as error:
        normalize_query_id(query_id)

    assert error.value.code == "invalid_query_id"
    assert query_id not in str(error.value) or query_id == ""


@pytest.mark.parametrize("payload", [b"***", b"YQ", b"YQ==="])
def test_rejects_noncanonical_base64(payload):
    assert_error_code(b"1 1:2 " + payload, "invalid_base64")


def test_rejects_uncompressed_thrift():
    raw = serialize_tree(make_tree())
    line = b"1 1:2 " + base64.b64encode(raw)

    assert_error_code(line, "invalid_zlib")


def test_rejects_truncated_zlib_stream():
    compressed = zlib.compress(serialize_tree(make_tree()))
    line = b"1 1:2 " + base64.b64encode(compressed[:-1])

    assert_error_code(line, "invalid_zlib")


def test_rejects_trailing_zlib_stream_data():
    compressed = zlib.compress(serialize_tree(make_tree())) + b"trailing"
    line = b"1 1:2 " + base64.b64encode(compressed)

    assert_error_code(line, "invalid_zlib")


def test_enforces_decoded_size_before_deserializing():
    raw = serialize_tree(make_tree())

    with pytest.raises(ProfileError) as error:
        decode_line(encode_raw(raw), max_decoded_bytes=len(raw) - 1)

    assert error.value.code == "decoded_size_limit"


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_rejects_invalid_decoded_size_limit(limit):
    with pytest.raises(ProfileError) as error:
        decode_line(V1_LINE, max_decoded_bytes=limit)

    assert error.value.code == "invalid_size_limit"


def test_accepts_payload_exactly_at_decoded_size_limit():
    raw = serialize_tree(make_tree())

    assert decode_line(encode_raw(raw), max_decoded_bytes=len(raw)).tree.nodes[0].name == "Query"


def test_rejects_truncated_compact_thrift():
    raw = serialize_tree(make_tree())

    assert_error_code(encode_raw(raw[:-1]), "invalid_thrift")


def test_rejects_bytes_after_compact_thrift_tree():
    raw = serialize_tree(make_tree())

    assert_error_code(encode_raw(raw + b"\x00"), "trailing_thrift_data")


def test_rejects_missing_required_tree_field():
    assert_error_code(encode_profile_line(make_tree(nodes=None).__class__()), "invalid_thrift")


def test_rejects_missing_required_nested_field():
    counter = TCounter(name="RowsReturned", unit=0, value=None)
    tree = make_tree(nodes=[make_node(counters=[counter])])

    assert_error_code(encode_profile_line(tree), "invalid_thrift")


@pytest.mark.parametrize(
    "nodes",
    [
        [],
        [make_node(num_children=-1)],
        [make_node(num_children=1)],
        [make_node(num_children=0), make_node(name="Orphan")],
        [make_node(num_children=2), make_node(name="Only child")],
    ],
)
def test_rejects_nodes_that_do_not_form_one_preorder_tree(nodes):
    assert_error_code(encode_profile_line(make_tree(nodes=nodes)), "invalid_tree")


@pytest.mark.parametrize("version", [0, 3, -1, 2**31 - 1])
def test_rejects_unsupported_profile_version(version):
    assert_error_code(
        encode_profile_line(make_tree(profile_version=version)),
        "unsupported_profile_version",
    )


def test_accepts_well_formed_v2_aggregated_parallel_arrays():
    aggregated = make_aggregated()
    tree = make_tree(nodes=[make_node(aggregated=aggregated)], profile_version=2)

    decoded = decode_line(encode_profile_line(tree))

    assert decoded.tree.profile_version == 2
    assert decoded.tree.nodes[0].aggregated.num_instances == 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("input_profiles", ["one"]),
        ("counters", [TAggCounter(name="C", unit=0, has_value=[True], values=[1, 2])]),
        (
            "summary_stats_counters",
            [
                TAggSummaryStatsCounter(
                    name="S",
                    unit=0,
                    has_value=[True, True],
                    sum=[1],
                    total_num_values=[1, 1],
                    min_value=[1, 1],
                    max_value=[1, 1],
                )
            ],
        ),
        (
            "time_series_counters",
            [TAggTimeSeriesCounter(name="T", unit=0, period_ms=[1], values=[[1], [2]], start_index=[0, 0])],
        ),
        (
            "event_sequences",
            [TAggEventSequence(name="E", label_dict=["a"], label_idxs=[[0], [0]], timestamps=[[1]])],
        ),
    ],
)
def test_rejects_v2_aggregated_arrays_not_parallel_to_instances(field, value):
    aggregated = make_aggregated()
    setattr(aggregated, field, value)
    tree = make_tree(nodes=[make_node(aggregated=aggregated)], profile_version=2)

    assert_error_code(encode_profile_line(tree), "invalid_aggregated_profile")


def test_rejects_v2_event_labels_outside_dictionary():
    aggregated = make_aggregated()
    aggregated.event_sequences[0].label_idxs[1] = [1]
    tree = make_tree(nodes=[make_node(aggregated=aggregated)], profile_version=2)

    assert_error_code(encode_profile_line(tree), "invalid_aggregated_profile")


def test_rejects_event_sequence_timestamp_label_mismatch():
    sequence = TEventSequence(name="Events", timestamps=[10], labels=["one", "two"])
    tree = make_tree(nodes=[make_node(event_sequences=[sequence])])

    assert_error_code(encode_profile_line(tree), "invalid_event_sequence")


def test_rejects_declared_container_size_before_allocating_items():
    # Tree field 1 (nodes), compact list header, then unsigned varint 100001.
    raw = b"\x19\xfc\xa1\x8d\x06"

    assert_error_code(encode_raw(raw), "container_size_limit")


def test_rejects_declared_string_size_before_reading_it():
    # Unknown tree field 4 encoded as string, then unsigned varint 16 MiB + 1.
    raw = b"\x48\x81\x80\x80\x08"

    assert_error_code(encode_raw(raw), "string_size_limit")


def test_rejects_excessive_unknown_struct_nesting():
    # Unknown field 4 is a struct; each nested struct contains field 1 as a struct.
    raw = b"\x4c" + (b"\x1c" * 64) + (b"\x00" * 65) + b"\x00"

    assert_error_code(encode_raw(raw), "thrift_depth_limit")


def test_rejects_unterminated_unknown_i64_varint_at_ten_bytes():
    raw = with_unknown_tree_i64_varint((b"\xff" * 100_000) + b"\x01")

    assert_error_code(encode_raw(raw), "integer_size_limit")


def test_rejects_unknown_i64_one_bit_wider_than_unsigned_64():
    raw = with_unknown_tree_i64_varint((b"\x80" * 9) + b"\x02")

    assert_error_code(encode_raw(raw), "integer_size_limit")


@pytest.mark.parametrize(
    "encoded",
    [
        b"\xfe\xff\xff\xff\xff\xff\xff\xff\xff\x01",
        b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01",
    ],
)
def test_accepts_unknown_i64_at_signed_64_boundaries(encoded):
    profile = decode_line(encode_raw(with_unknown_tree_i64_varint(encoded)))

    assert profile.tree.nodes[0].name == "Query"


def test_rejects_5000_digit_timestamp_before_integer_conversion():
    payload = V1_LINE.split()[2]

    assert_error_code((b"9" * 5_000) + b" 1:2 " + payload, "invalid_timestamp")


def make_aggregated():
    return TAggregatedRuntimeProfileNode(
        num_instances=2,
        input_profiles=["one", "two"],
        counters=[TAggCounter(name="C", unit=0, has_value=[True, False], values=[1, 0])],
        info_strings={"Key": {"same": [0, 1]}},
        summary_stats_counters=[
            TAggSummaryStatsCounter(
                name="S",
                unit=0,
                has_value=[True, True],
                sum=[1, 2],
                total_num_values=[1, 1],
                min_value=[1, 2],
                max_value=[1, 2],
            )
        ],
        event_sequences=[
            TAggEventSequence(
                name="E",
                label_dict=["a"],
                label_idxs=[[0], [0]],
                timestamps=[[1], [2]],
            )
        ],
        time_series_counters=[
            TAggTimeSeriesCounter(
                name="T",
                unit=0,
                period_ms=[1, 1],
                values=[[1], [2]],
                start_index=[0, 0],
            )
        ],
    )


def with_unknown_tree_i64_varint(encoded):
    tree = serialize_tree(make_tree())
    assert tree.endswith(b"\x00")
    # Tree field 4 follows field 1: delta 3, compact I64 type 6.
    return tree[:-1] + b"\x36" + encoded + b"\x00"


def assert_error_code(line, expected_code):
    with pytest.raises(ProfileError) as error:
        decode_line(line)

    assert error.value.code == expected_code
    assert str(error.value) == expected_code
