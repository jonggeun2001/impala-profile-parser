"""Write a synthetic Impala 3.4 wire fixture independently of the bundled reader.

Field IDs/types follow Apache Impala tag 3.4.0 common/thrift/RuntimeProfile.thrift.
Uses the Apache Thrift compact writer directly, no generated or production types.
Run under the development environment. This is not a captured CDP production log.
"""

import base64
from pathlib import Path
import zlib

from thrift.Thrift import TType
from thrift.protocol.TCompactProtocol import TCompactProtocol
from thrift.transport.TTransport import TMemoryBuffer


def field(protocol, number, kind, write):
    protocol.writeFieldBegin("", kind, number)
    write()
    protocol.writeFieldEnd()


def node(protocol, name, children, info, rows=None):
    protocol.writeStructBegin("TRuntimeProfileNode")
    field(protocol, 1, TType.STRING, lambda: protocol.writeString(name))
    field(protocol, 2, TType.I32, lambda: protocol.writeI32(children))

    def counters():
        protocol.writeListBegin(TType.STRUCT, 0 if rows is None else 1)
        if rows is not None:
            protocol.writeStructBegin("TCounter")
            field(protocol, 1, TType.STRING, lambda: protocol.writeString("NumRowsFetched"))
            field(protocol, 2, TType.I32, lambda: protocol.writeI32(0))  # TUnit.UNIT
            field(protocol, 3, TType.I64, lambda: protocol.writeI64(rows))
            protocol.writeFieldStop()
            protocol.writeStructEnd()
        protocol.writeListEnd()

    field(protocol, 3, TType.LIST, counters)
    field(protocol, 4, TType.I64, lambda: protocol.writeI64(-1))
    field(protocol, 5, TType.BOOL, lambda: protocol.writeBool(True))

    def strings():
        protocol.writeMapBegin(TType.STRING, TType.STRING, len(info))
        for key, value in info.items():
            protocol.writeString(key)
            protocol.writeString(value)
        protocol.writeMapEnd()

    field(protocol, 6, TType.MAP, strings)

    def order():
        protocol.writeListBegin(TType.STRING, len(info))
        for key in info:
            protocol.writeString(key)
        protocol.writeListEnd()

    field(protocol, 7, TType.LIST, order)

    def child_counters():
        protocol.writeMapBegin(TType.STRING, TType.SET, 0)
        protocol.writeMapEnd()

    field(protocol, 8, TType.MAP, child_counters)
    protocol.writeFieldStop()
    protocol.writeStructEnd()


def main():
    transport = TMemoryBuffer()
    protocol = TCompactProtocol(transport)
    protocol.writeStructBegin("TRuntimeProfileTree")
    protocol.writeFieldBegin("nodes", TType.LIST, 1)
    protocol.writeListBegin(TType.STRUCT, 3)
    node(protocol, "Query (id=1:2)", 2, {})
    node(protocol, "Summary", 0, {
        "Impala Version": "impalad version 3.4.0 RELEASE",
        "User": "fixture_user", "Default Db": "default", "Query Type": "QUERY",
        "Sql Statement": "select\n  42 -- synthetic fixture",
        "Query State": "FINISHED", "Query Status": "OK", "Request Pool": "root.default",
        "Start Time": "2021-08-05 10:00:00.000000000",
        "End Time": "2021-08-05 10:00:01.250000000",
    })
    node(protocol, "ImpalaServer", 0, {}, rows=1)
    protocol.writeListEnd()
    protocol.writeFieldEnd()
    # Impala 3.4 has no profile_version field; default representation is version 1.
    protocol.writeFieldStop()
    protocol.writeStructEnd()
    target = Path(__file__).resolve().parents[1] / "tests/fixtures/impala-3.4-synthetic.log"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"1628154001250 1:2 " + base64.b64encode(zlib.compress(transport.getvalue())) + b"\n")


if __name__ == "__main__":
    main()
