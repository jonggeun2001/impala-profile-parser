"""Reusable real-Thrift fixtures for parser tests."""

import base64
import zlib

from thrift.protocol import TCompactProtocol
from thrift.transport import TTransport

from impala_profile_parser.vendor.RuntimeProfile.ttypes import (
    TRuntimeProfileNode,
    TRuntimeProfileTree,
)


def make_node(name="Query", num_children=0, **overrides):
    values = {
        "name": name,
        "num_children": num_children,
        "counters": [],
        "metadata": -1,
        "indent": False,
        "info_strings": {},
        "info_strings_display_order": [],
        "child_counters_map": {},
    }
    values.update(overrides)
    return TRuntimeProfileNode(**values)


def make_tree(nodes=None, profile_version=None):
    if nodes is None:
        nodes = [make_node()]
    return TRuntimeProfileTree(nodes=nodes, profile_version=profile_version)


def serialize_tree(tree):
    transport = TTransport.TMemoryBuffer()
    protocol = TCompactProtocol.TCompactProtocol(transport)
    tree.write(protocol)
    return transport.getvalue()


def encode_raw(raw, timestamp=1234, query_id="1:2"):
    payload = base64.b64encode(zlib.compress(raw))
    return str(timestamp).encode("ascii") + b" " + query_id.encode("ascii") + b" " + payload


def encode_profile_line(tree, timestamp=1234, query_id="1:2"):
    return encode_raw(serialize_tree(tree), timestamp=timestamp, query_id=query_id)
