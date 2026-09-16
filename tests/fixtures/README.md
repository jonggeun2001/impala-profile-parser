# Compatibility fixture

`impala-3.4-synthetic.log` is synthetic, not a captured production query.
It uses the field IDs and types of Apache Impala 3.4.0 RuntimeProfile.thrift,
including the absence of `profile_version` in that version.

The deterministic generator `scripts/generate_compat_fixture.py` writes compact
Thrift directly using Apache Thrift; it does not import our decoder or vendored
generated types. This checks backward wire compatibility independently of the
newer generated reader. The payload contains only fictional query data.

Regenerate with `.venv/bin/python scripts/generate_compat_fixture.py`.

Source schema: https://github.com/apache/impala/blob/3.4.0/common/thrift/RuntimeProfile.thrift

The CLI round-trip test asserts query ID, SQL preservation, client-fetched rows,
and 1250 ms elapsed time. It does not establish compatibility with production logs
or distribution-specific changes.
