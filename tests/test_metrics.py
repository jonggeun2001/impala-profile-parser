from types import SimpleNamespace

import pyarrow as pa
import pytest

from impala_profile_parser.vendor.Metrics.ttypes import TUnit


QUERY_ID = "0000000000000001:0000000000000002"


def node(name, *, children=0, info=None, counters=None):
    return SimpleNamespace(
        name=name,
        num_children=children,
        info_strings={} if info is None else info,
        counters=[] if counters is None else counters,
    )


def counter(name, value, unit):
    return SimpleNamespace(name=name, value=value, unit=unit)


def profile(nodes, *, version=None, logged_at_ms=1_700_000_000_123):
    return SimpleNamespace(
        query_id=QUERY_ID,
        logged_at_ms=logged_at_ms,
        tree=SimpleNamespace(nodes=nodes, profile_version=version),
    )


def extract(nodes, **kwargs):
    from impala_profile_parser.metrics import extract_record

    return extract_record(
        profile(nodes, **kwargs), "logs/profile.log", 17, timezone="UTC"
    )


def test_schema_has_fixed_fields_nullability_and_metadata():
    from impala_profile_parser.schema import SCHEMA

    expected = pa.schema(
        [
            pa.field("queryId", pa.string(), nullable=False),
            pa.field("profileLoggedAt", pa.timestamp("ms", tz="UTC"), nullable=False),
            pa.field("sourceFile", pa.string(), nullable=False),
            pa.field("sourceLine", pa.int64(), nullable=False),
            pa.field("profileVersion", pa.int32(), nullable=False),
            pa.field("parseStatus", pa.string(), nullable=False),
            pa.field("warningCount", pa.int64(), nullable=False),
            pa.field("impalaVersion", pa.string()),
            pa.field("user", pa.string()),
            pa.field("defaultDatabase", pa.string()),
            pa.field("statementType", pa.string()),
            pa.field("query", pa.string()),
            pa.field("requestPool", pa.string()),
            pa.field("queryState", pa.string()),
            pa.field("queryStatus", pa.string()),
            pa.field("startTime", pa.timestamp("ms", tz="UTC")),
            pa.field("endTime", pa.timestamp("ms", tz="UTC")),
            pa.field("durationMilliseconds", pa.int64()),
            pa.field("resultRows", pa.int64()),
            pa.field("resultRowsKind", pa.string()),
            pa.field("resultRowsSource", pa.string()),
            pa.field("cpuMilliseconds", pa.int64()),
            pa.field("hdfsBytesRead", pa.int64()),
            pa.field("spillBytesWritten", pa.int64()),
            pa.field("maxBackendPeakMemoryBytes", pa.int64()),
        ]
    )
    assert SCHEMA.remove_metadata() == expected
    assert SCHEMA.metadata == {
        b"impala.schema.version": b"1",
        b"impala.parser.version": b"0.1.0",
        b"impala.mapping.version": b"1",
    }


def test_extracts_only_query_scopes_and_preserves_sql_exactly():
    sql = "select\n  1 -- comment"
    summary = node(
        "Summary",
        children=1,
        info={
            "Impala Version": "impalad version 3.4.0.7.1.7.0-551",
            "User": "alice",
            "Default Db": "analytics",
            "Query Type": "QUERY",
            "Sql Statement": sql,
            "Request Pool": "root.default",
            "Impala Query State": "FINISHED",
            "Query State": "EXCEPTION",
            "Query Status": "OK",
        },
    )
    nested = node(
        "SCAN HDFS",
        info={"User": "wrong", "Sql Statement": "select secret"},
        counters=[counter("RowsReturned", 999, TUnit.UNIT)],
    )
    row = extract([node(f"Query (id={QUERY_ID})", children=1), summary, nested])

    assert row["queryId"] == QUERY_ID
    assert row["profileLoggedAt"] == 1_700_000_000_123
    assert row["sourceFile"] == "logs/profile.log"
    assert row["sourceLine"] == 17
    assert row["profileVersion"] == 1
    assert row["impalaVersion"] == "impalad version 3.4.0.7.1.7.0-551"
    assert row["user"] == "alice"
    assert row["defaultDatabase"] == "analytics"
    assert row["statementType"] == "QUERY"
    assert row["query"] == sql
    assert row["requestPool"] == "root.default"
    assert row["queryState"] == "FINISHED"
    assert row["queryStatus"] == "OK"
    assert row["resultRows"] is None
    assert row["parseStatus"] == "OK"
    assert row["warningCount"] == 0


def test_root_summary_info_is_allowed_and_missing_fields_do_not_warn():
    row = extract(
        [node(f"Query (id={QUERY_ID})", info={"User": "root-user"})], version=2
    )

    assert row["user"] == "root-user"
    assert row["profileVersion"] == 2
    assert row["parseStatus"] == "OK"
    assert row["warningCount"] == 0
    assert row["hdfsBytesRead"] is None
    assert row["spillBytesWritten"] is None
    assert row["maxBackendPeakMemoryBytes"] is None


def test_conflicting_summary_metadata_is_null_and_warns_instead_of_last_win():
    row = extract(
        [
            node(f"Query (id={QUERY_ID})", children=2, info={"User": "alice"}),
            node("Summary", info={"User": "bob"}),
            node("Summary", info={"User": "charlie"}),
        ]
    )

    assert row["user"] is None
    assert row["parseStatus"] == "WARNING"
    assert row["warningCount"] == 1


def test_result_rows_are_client_fetched_even_for_failed_query_and_zero_is_kept():
    row = extract(
        [
            node(f"Query (id={QUERY_ID})", children=2),
            node("Summary", info={"Impala Query State": "ERROR"}),
            node(
                "ImpalaServer",
                counters=[counter("NumRowsFetched", 0, TUnit.UNIT)],
            ),
        ]
    )

    assert row["resultRows"] == 0
    assert row["resultRowsKind"] == "CLIENT_FETCHED"
    assert row["resultRowsSource"] == "ImpalaServer/NumRowsFetched"
    assert row["queryState"] == "ERROR"


def test_counter_wrong_unit_or_duplicate_scope_is_null_with_warning():
    wrong_unit = extract(
        [
            node(f"Query (id={QUERY_ID})", children=1),
            node(
                "ImpalaServer",
                counters=[counter("NumRowsFetched", 7, TUnit.BYTES)],
            ),
        ]
    )
    duplicate_scope = extract(
        [
            node(f"Query (id={QUERY_ID})", children=2),
            node(
                "ImpalaServer",
                counters=[counter("NumRowsFetched", 7, TUnit.UNIT)],
            ),
            node(
                "ImpalaServer",
                counters=[counter("NumRowsFetched", 8, TUnit.UNIT)],
            ),
        ]
    )

    assert wrong_unit["resultRows"] is None
    assert wrong_unit["warningCount"] == 1
    assert duplicate_scope["resultRows"] is None
    assert duplicate_scope["warningCount"] == 1


def test_cpu_uses_only_exact_execution_profile_counter_and_truncates_ns():
    execution_name = f"Execution Profile {QUERY_ID}"
    row = extract(
        [
            node(f"Query (id={QUERY_ID})", children=2),
            node(
                execution_name,
                children=1,
                counters=[counter("TotalCpuTime", 3_999_999, TUnit.TIME_NS)],
            ),
            node(
                "Fragment 0",
                counters=[counter("TotalCpuTime", 999_000_000, TUnit.TIME_NS)],
            ),
            node(
                "Execution Profile unrelated",
                counters=[counter("TotalCpuTime", 555_000_000, TUnit.TIME_NS)],
            ),
        ]
    )

    assert row["cpuMilliseconds"] == 3


def test_average_and_instance_counters_are_never_used():
    execution = node(f"Execution Profile {QUERY_ID}")
    execution.counters = []
    execution.aggregated = SimpleNamespace(
        counters=[counter("TotalCpuTime", 77_000_000, TUnit.TIME_NS)]
    )
    execution.summary_stats_counters = [
        counter("TotalCpuTime", 88_000_000, TUnit.TIME_NS)
    ]
    row = extract([node(f"Query (id={QUERY_ID})", children=1), execution])

    assert row["cpuMilliseconds"] is None
    assert row["warningCount"] == 0


@pytest.mark.parametrize(
    "scope_name,counter_name,counter_value,unit,result_column",
    [
        ("ImpalaServer", "NumRowsFetched", 9, TUnit.UNIT, "resultRows"),
        (
            f"Execution Profile {QUERY_ID}",
            "TotalCpuTime",
            9_000_000,
            TUnit.TIME_NS,
            "cpuMilliseconds",
        ),
    ],
)
def test_scalar_counter_on_aggregated_query_scope_is_rejected(
    scope_name, counter_name, counter_value, unit, result_column
):
    scope = node(
        scope_name,
        counters=[counter(counter_name, counter_value, unit)],
    )
    scope.aggregated = SimpleNamespace(num_instances=2)

    row = extract([node(f"Query (id={QUERY_ID})", children=1), scope])

    assert row[result_column] is None
    assert row["parseStatus"] == "WARNING"
    assert row["warningCount"] == 1


def test_naive_nanosecond_timestamps_use_requested_zone_and_trim_to_ms():
    from impala_profile_parser.metrics import extract_record

    nodes = [
        node(f"Query (id={QUERY_ID})", children=1),
        node(
            "Summary",
            info={
                "Start Time": "2026-01-02 03:04:05.123999999",
                "End Time": "2026-01-02 03:04:06.987999999",
            },
        ),
    ]
    row = extract_record(
        profile(nodes), "profile.log", 1, timezone="Asia/Seoul"
    )

    assert row["startTime"] == 1_767_290_645_123
    assert row["endTime"] == 1_767_290_646_987
    assert row["durationMilliseconds"] == 1_864
    assert row["warningCount"] == 0


@pytest.mark.parametrize(
    "start,end,zone,expected_warnings",
    [
        ("not-a-time", None, "UTC", 1),
        ("2026-11-01 01:30:00", None, "America/New_York", 1),
        ("2026-03-08 02:30:00", None, "America/New_York", 1),
        ("2026-01-01 00:00:00", None, "Invalid/Zone", 1),
        ("2026-01-02 00:00:00", "2026-01-01 00:00:00", "UTC", 1),
    ],
)
def test_bad_ambiguous_nonexistent_zone_and_reversed_times_warn(
    start, end, zone, expected_warnings
):
    from impala_profile_parser.metrics import extract_record

    info = {"Start Time": start}
    if end is not None:
        info["End Time"] = end
    row = extract_record(
        profile(
            [node(f"Query (id={QUERY_ID})", children=1), node("Summary", info=info)]
        ),
        "profile.log",
        1,
        timezone=zone,
    )

    if end is None:
        assert row["startTime"] is None
    assert row["durationMilliseconds"] is None
    assert row["parseStatus"] == "WARNING"
    assert row["warningCount"] == expected_warnings


@pytest.mark.parametrize(
    "root_name,summary_info,execution_name",
    [
        ("Query (id=3:4)", {}, None),
        (f"Query (id={QUERY_ID})", {"Query Id": "3:4"}, None),
        (f"Query (id={QUERY_ID})", {}, "Execution Profile 3:4"),
    ],
)
def test_internal_query_id_mismatch_is_a_profile_error_without_payload_data(
    root_name, summary_info, execution_name
):
    from impala_profile_parser.decoder import ProfileError
    from impala_profile_parser.metrics import extract_record

    direct = [node("Summary", info=summary_info)]
    if execution_name is not None:
        direct.append(node(execution_name))
    nodes = [node(root_name, children=len(direct)), *direct]

    with pytest.raises(ProfileError) as caught:
        extract_record(profile(nodes), "secret.sql.log", 9, timezone="UTC")
    message = str(caught.value)
    assert "select" not in message.lower()
    assert QUERY_ID not in message


def test_malformed_explicit_internal_query_id_is_a_profile_error():
    from impala_profile_parser.decoder import ProfileError

    with pytest.raises(ProfileError):
        extract([node("Query (id=not-an-id)")])
