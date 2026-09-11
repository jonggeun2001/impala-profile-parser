"""Conservative extraction of query-level fields from runtime profile trees."""

import re
from datetime import datetime, timedelta, timezone as datetime_timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from impala_profile_parser.decoder import ProfileError, normalize_query_id
from impala_profile_parser.vendor.Metrics.ttypes import TUnit


_ROOT_QUERY_ID = re.compile(r"^Query \(id=(.*)\)$")
_EXECUTION_QUERY_ID = re.compile(
    r"^Execution Profile ([0-9a-fA-F]{1,16}:[0-9a-fA-F]{1,16})$"
)
_TIMESTAMP = re.compile(
    r"^(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})"
    r"(?:\.(\d{1,9}))?(?:\s*(Z|[+-]\d{2}:\d{2}))?$"
)
_UTC_EPOCH = datetime(1970, 1, 1, tzinfo=datetime_timezone.utc)

_SUMMARY_FIELDS = {
    "impalaVersion": "Impala Version",
    "user": "User",
    "defaultDatabase": "Default Db",
    "statementType": "Query Type",
    "query": "Sql Statement",
    "requestPool": "Request Pool",
    "queryStatus": "Query Status",
}


def _subtree_end(nodes: Sequence[Any], start: int) -> int:
    """Return the first preorder index after ``start`` and all its descendants."""
    pending = 1
    index = start
    while pending and index < len(nodes):
        current = nodes[index]
        pending -= 1
        pending += max(0, int(getattr(current, "num_children", 0) or 0))
        index += 1
    return index


def _direct_children(nodes: Sequence[Any]) -> List[Any]:
    if not nodes:
        return []
    result = []
    index = 1
    count = max(0, int(getattr(nodes[0], "num_children", 0) or 0))
    for _ in range(count):
        if index >= len(nodes):
            break
        result.append(nodes[index])
        index = _subtree_end(nodes, index)
    return result


def _info_values(scopes: Iterable[Any], key: str) -> List[Any]:
    values = []
    for scope in scopes:
        info = getattr(scope, "info_strings", None) or {}
        if key in info:
            values.append(info[key])
    return values


def _resolve_info(scopes: Iterable[Any], key: str) -> Tuple[Optional[str], bool]:
    values = _info_values(scopes, key)
    if not values:
        return None, False
    if any(not isinstance(value, str) for value in values):
        return None, True
    unique = set(values)
    if len(unique) != 1:
        return None, True
    return values[0], False


def _validate_internal_query_ids(
    envelope_query_id: str, root: Any, summary_scopes: Iterable[Any], direct: Iterable[Any]
) -> None:
    root_name = getattr(root, "name", "")
    root_match = _ROOT_QUERY_ID.fullmatch(root_name)
    if root_match is not None:
        if normalize_query_id(root_match.group(1)) != envelope_query_id:
            raise ProfileError("query_id_mismatch")

    for key in ("Query Id", "Query ID"):
        for value in _info_values(summary_scopes, key):
            if normalize_query_id(value) != envelope_query_id:
                raise ProfileError("query_id_mismatch")

    for scope in direct:
        match = _EXECUTION_QUERY_ID.fullmatch(getattr(scope, "name", ""))
        if match is not None and normalize_query_id(match.group(1)) != envelope_query_id:
            raise ProfileError("query_id_mismatch")


def _counter_value(
    scopes: Iterable[Any], name: str, unit: int
) -> Tuple[Optional[int], bool]:
    matches = []
    for scope in scopes:
        for candidate in getattr(scope, "counters", None) or []:
            if getattr(candidate, "name", None) == name:
                if getattr(scope, "aggregated", None) is not None:
                    return None, True
                matches.append(candidate)
    if not matches:
        return None, False

    values = []
    for candidate in matches:
        value = getattr(candidate, "value", None)
        if (
            getattr(candidate, "unit", None) != unit
            or not isinstance(value, int)
            or isinstance(value, bool)
            or value < 0
        ):
            return None, True
        values.append(value)
    if len(set(values)) != 1:
        return None, True
    return values[0], False


def _parse_offset(value: str) -> datetime_timezone:
    if value == "Z":
        return datetime_timezone.utc
    sign = 1 if value[0] == "+" else -1
    hours = int(value[1:3])
    minutes = int(value[4:6])
    if hours > 23 or minutes > 59:
        raise ValueError("invalid offset")
    return datetime_timezone(sign * timedelta(hours=hours, minutes=minutes))


def _localize(naive: datetime, zone: ZoneInfo) -> Optional[datetime]:
    candidates = []
    for fold in (0, 1):
        aware = naive.replace(tzinfo=zone, fold=fold)
        round_trip = aware.astimezone(datetime_timezone.utc).astimezone(zone)
        if round_trip.replace(tzinfo=None) == naive:
            candidates.append(aware)
    if not candidates:
        return None
    if len(candidates) == 2 and candidates[0].utcoffset() != candidates[1].utcoffset():
        return None
    return candidates[0]


def _epoch_milliseconds(value: str, timezone_name: str) -> Optional[int]:
    if not isinstance(value, str):
        return None
    match = _TIMESTAMP.fullmatch(value)
    if match is None:
        return None
    base, fraction, offset = match.groups()
    try:
        naive = datetime.strptime(base.replace("T", " "), "%Y-%m-%d %H:%M:%S")
        milliseconds = int(((fraction or "") + "000")[:3])
        naive = naive.replace(microsecond=milliseconds * 1_000)
        if offset is not None:
            aware = naive.replace(tzinfo=_parse_offset(offset))
        else:
            try:
                zone = ZoneInfo(timezone_name)
            except (ZoneInfoNotFoundError, ValueError, TypeError):
                return None
            aware = _localize(naive, zone)
            if aware is None:
                return None
        delta = aware.astimezone(datetime_timezone.utc) - _UTC_EPOCH
        return (
            delta.days * 86_400_000
            + delta.seconds * 1_000
            + delta.microseconds // 1_000
        )
    except (OverflowError, TypeError, ValueError):
        return None


def _timestamp_field(
    scopes: Iterable[Any], key: str, timezone_name: str
) -> Tuple[Optional[int], bool]:
    raw, warning = _resolve_info(scopes, key)
    if warning or raw is None:
        return None, warning
    parsed = _epoch_milliseconds(raw, timezone_name)
    return parsed, parsed is None


def extract_record(
    profile: Any,
    source_file: str,
    source_line: int,
    timezone: str = "UTC",
) -> Dict[str, Any]:
    """Map a decoded profile snapshot to one record matching :data:`SCHEMA`."""
    nodes = profile.tree.nodes
    root = nodes[0]
    direct = _direct_children(nodes)
    summary_scopes = [root] + [scope for scope in direct if scope.name == "Summary"]
    envelope_query_id = normalize_query_id(profile.query_id)
    _validate_internal_query_ids(envelope_query_id, root, summary_scopes, direct)

    warnings = 0
    record: Dict[str, Any] = {
        "queryId": envelope_query_id,
        "profileLoggedAt": profile.logged_at_ms,
        "sourceFile": source_file,
        "sourceLine": source_line,
        "profileVersion": getattr(profile.tree, "profile_version", None) or 1,
        "parseStatus": "OK",
        "warningCount": 0,
        "impalaVersion": None,
        "user": None,
        "defaultDatabase": None,
        "statementType": None,
        "query": None,
        "requestPool": None,
        "queryState": None,
        "queryStatus": None,
        "startTime": None,
        "endTime": None,
        "durationMilliseconds": None,
        "resultRows": None,
        "resultRowsKind": None,
        "resultRowsSource": None,
        "cpuMilliseconds": None,
        "hdfsBytesRead": None,
        "spillBytesWritten": None,
        "maxBackendPeakMemoryBytes": None,
    }

    for column, info_key in _SUMMARY_FIELDS.items():
        value, warning = _resolve_info(summary_scopes, info_key)
        record[column] = value
        warnings += int(warning)

    preferred_states = _info_values(summary_scopes, "Impala Query State")
    state_key = "Impala Query State" if preferred_states else "Query State"
    record["queryState"], warning = _resolve_info(summary_scopes, state_key)
    warnings += int(warning)

    record["startTime"], warning = _timestamp_field(
        summary_scopes, "Start Time", timezone
    )
    warnings += int(warning)
    record["endTime"], warning = _timestamp_field(summary_scopes, "End Time", timezone)
    warnings += int(warning)
    if record["startTime"] is not None and record["endTime"] is not None:
        if record["endTime"] >= record["startTime"]:
            record["durationMilliseconds"] = record["endTime"] - record["startTime"]
        else:
            warnings += 1

    server_scopes = [scope for scope in direct if scope.name == "ImpalaServer"]
    result_rows, warning = _counter_value(server_scopes, "NumRowsFetched", TUnit.UNIT)
    warnings += int(warning)
    if result_rows is not None:
        record["resultRows"] = result_rows
        record["resultRowsKind"] = "CLIENT_FETCHED"
        record["resultRowsSource"] = "ImpalaServer/NumRowsFetched"

    execution_scopes = []
    for scope in direct:
        match = _EXECUTION_QUERY_ID.fullmatch(getattr(scope, "name", ""))
        if match is not None and normalize_query_id(match.group(1)) == envelope_query_id:
            execution_scopes.append(scope)
    cpu_ns, warning = _counter_value(execution_scopes, "TotalCpuTime", TUnit.TIME_NS)
    warnings += int(warning)
    if cpu_ns is not None:
        record["cpuMilliseconds"] = cpu_ns // 1_000_000

    record["warningCount"] = warnings
    if warnings:
        record["parseStatus"] = "WARNING"
    return record
