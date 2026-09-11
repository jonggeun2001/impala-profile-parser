# Output schema and metric mapping

`result.parquet` contains one row for every non-empty input profile record. It is
a snapshot table, not a query table: repeated log lines, duplicate query IDs, and
multiple snapshots of one query remain separate rows. Consumers can use
`sourceFile` and `sourceLine` to locate a snapshot and `profileLoggedAt` to order
snapshots. The parser does not deduplicate or merge them.

The mapping targets CDP Private Cloud Base 7.1.7, whose documented Impala
component is `3.4.0.7.1.7.0-551`. The vendored Impala 4.6 generated Thrift reader
is used only because older optional fields retain their wire field numbers and
types. Compatibility with every CDP service pack and hotfix still needs
validation against real operational logs.

## Columns

Schema version 2 contains 22 columns. Unimplemented metrics are omitted rather
than emitted as permanently null columns.

All timestamp columns are Arrow `timestamp[ms, tz=UTC]`. All metric/count
columns are signed Arrow `int64`; absent metrics remain null and meaningful zero
values are preserved.

| Column | Arrow type | Required | Profile source and meaning |
|---|---|---:|---|
| `queryId` | string | yes | Canonical, lowercase, zero-padded `16hex:16hex` ID from the log envelope. An explicit ID in the root name, Summary information, or direct-child Execution Profile must match it. |
| `profileLoggedAt` | timestamp(ms, UTC) | yes | Epoch milliseconds from the log envelope. This is the snapshot log time, not query end time. |
| `sourceFile` | string | yes | Safe display path supplied by the caller. |
| `sourceLine` | int64 | yes | One-based input line supplied by the caller. |
| `profileVersion` | int32 | yes | Thrift tree `profile_version`; missing means version 1. |
| `parseStatus` | string | yes | `OK` when no optional-field warning occurred, otherwise `WARNING`. Structural decoding errors do not produce a row. |
| `warningCount` | int64 | yes | Number of malformed or conflicting optional mappings in the row. A missing optional value alone is not a warning. |
| `impalaVersion` | string | no | `Impala Version` from root/Summary query information. |
| `user` | string | no | `User` from root/Summary query information. |
| `defaultDatabase` | string | no | `Default Db` from root/Summary query information. |
| `statementType` | string | no | `Query Type` from root/Summary query information. |
| `query` | string | no | `Sql Statement` from root/Summary query information, preserved exactly as recorded, including whitespace, newlines, comments, or redaction text. |
| `requestPool` | string | no | `Request Pool` from root/Summary query information. |
| `queryState` | string | no | `Impala Query State` when present, otherwise `Query State`, from root/Summary query information. |
| `queryStatus` | string | no | `Query Status` from root/Summary query information. |
| `startTime` | timestamp(ms, UTC) | no | Parsed `Start Time` from root/Summary query information. |
| `endTime` | timestamp(ms, UTC) | no | Parsed `End Time` from root/Summary query information. |
| `durationMilliseconds` | int64 | no | `endTime - startTime` when both parse and end is not earlier than start. No profile duration counter is substituted. |
| `resultRows` | int64 | no | Direct-child `ImpalaServer/NumRowsFetched` with unit `UNIT`. It counts rows fetched by the client, including a valid zero; it is not total rows generated, returned by every operator, or committed by DML. It remains meaningful for partial or failed queries with that explicit interpretation. |
| `resultRowsKind` | string | no | `CLIENT_FETCHED` exactly when `resultRows` is populated. |
| `resultRowsSource` | string | no | `ImpalaServer/NumRowsFetched` exactly when `resultRows` is populated. |
| `cpuMilliseconds` | int64 | no | Direct-child `Execution Profile <queryId>/TotalCpuTime` with unit `TIME_NS`, divided by 1,000,000 with sub-millisecond remainder discarded. In Impala versions that emit it, this is the cumulative user-plus-system CPU represented by that counter. It is null when that exact counter is absent; no fragment, instance, average, or aggregated counters are summed. |

## Scope, conflicts, and time conversion

Summary strings are read only from the root and direct children named `Summary`.
Result rows are read only from direct children named `ImpalaServer`, and CPU is
read only from the direct child named `Execution Profile <queryId>`. Nested
operators and fragment or instance profiles cannot override these values.
Conflicting values across matching scopes, duplicate counters with conflicting
values, a wrong counter unit, and malformed optional values produce null plus a
warning; extraction never chooses the last value encountered. Average and
per-instance counter containers are not used. A matching scalar counter on a
node marked as aggregated is also rejected as null with a warning because it
cannot be interpreted as a query total. An absent counter remains null without
a warning.

Profile time strings accept `YYYY-MM-DD HH:MM:SS` (or a `T` separator), an
optional fraction of up to nine digits, and an optional `Z` or numeric UTC
offset. Fractions are explicitly truncated to milliseconds. A timestamp without
an offset uses the configured IANA timezone (UTC by default). Invalid strings,
unknown zones, and ambiguous or nonexistent daylight-saving local times become
null with a warning. If end precedes start, both parsed timestamps are retained,
but duration is null with a warning.

## Schema metadata

The Arrow schema stores these byte-string metadata keys:

| Key | Current value | Meaning |
|---|---|---|
| `impala.schema.version` | `2` | Column names, order, Arrow types, and nullability contract. |
| `impala.parser.version` | `0.0.1` | Package version that wrote the file. |
| `impala.mapping.version` | `1` | Profile scope, source counter, and semantic mapping contract. |

Version 2 removes the three unimplemented resource-metric columns from version 1.
Previously generated files keep their original schema until conversion is rerun.

Mapping evidence: the CDP 7.1.7 component version is published in the
[Cloudera runtime component table](https://docs.cloudera.com/cdp-private-cloud-base/7.1.7/runtime-release-notes/topics/rt-pvc-data-warehouse-component-versions.html).
Impala 3.4 source creates the direct-child `ImpalaServer` profile and increments
`NumRowsFetched` for client fetches in
[`client-request-state.cc`](https://github.com/apache/impala/blob/3.4.0/be/src/service/client-request-state.cc).
The coordinator creates `Execution Profile <queryId>` and exposes
`TotalCpuTime` when available in
[`coordinator.cc`](https://github.com/apache/impala/blob/3.4.0/be/src/runtime/coordinator.cc).
