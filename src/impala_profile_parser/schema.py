"""Stable Arrow schema for one decoded profile snapshot."""

import pyarrow as pa

from impala_profile_parser import __version__


SCHEMA_VERSION = "2"
MAPPING_VERSION = "1"


SCHEMA = pa.schema(
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
    ],
    metadata={
        b"impala.schema.version": SCHEMA_VERSION.encode("ascii"),
        b"impala.parser.version": __version__.encode("ascii"),
        b"impala.mapping.version": MAPPING_VERSION.encode("ascii"),
    },
)
