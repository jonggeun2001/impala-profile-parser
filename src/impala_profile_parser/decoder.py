"""Decode bounded, encoded impalad runtime-profile log records."""

import base64
import binascii
from dataclasses import dataclass
import re
import zlib

from thrift.protocol import TCompactProtocol
from thrift.transport import TTransport

from impala_profile_parser.vendor.RuntimeProfile.ttypes import (
    TRuntimeProfileNode,
    TRuntimeProfileTree,
)


_DEFAULT_MAX_DECODED_BYTES = 64 * 1024 * 1024
_MAX_STRING_BYTES = 16 * 1024 * 1024
_MAX_CONTAINER_ITEMS = 100_000
_MAX_NODES = 100_000
_MAX_THRIFT_DEPTH = 64
_MAX_INT64 = (1 << 63) - 1
_QUERY_ID_RE = re.compile(r"([0-9a-fA-F]{1,16}):([0-9a-fA-F]{1,16})\Z")


class ProfileError(ValueError):
    """A payload-safe decoder failure identified only by a stable reason code."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class DecodedProfile:
    query_id: str
    logged_at_ms: int
    tree: TRuntimeProfileTree


def normalize_query_id(query_id: str) -> str:
    """Return Impala's canonical lower-case, zero-padded query ID form."""
    if not isinstance(query_id, str):
        raise ProfileError("invalid_query_id")
    match = _QUERY_ID_RE.fullmatch(query_id)
    if match is None:
        raise ProfileError("invalid_query_id")
    return "%016x:%016x" % (int(match.group(1), 16), int(match.group(2), 16))


class _BytesTransport(TTransport.TTransportBase):
    def __init__(self, data):
        self._data = data
        self._position = 0

    def read(self, size):
        end = min(self._position + size, len(self._data))
        chunk = self._data[self._position:end]
        self._position = end
        return chunk

    @property
    def remaining(self):
        return len(self._data) - self._position


class _BoundedCompactProtocol(TCompactProtocol.TCompactProtocol):
    def __init__(self, transport):
        super().__init__(
            transport,
            string_length_limit=_MAX_STRING_BYTES,
            container_length_limit=_MAX_CONTAINER_ITEMS,
        )
        self._depth = 0

    # Apache Thrift 0.16 routes compact integers, field IDs, and sizes through
    # this private method. Pinning the mangled name is intentional: the stock
    # implementation has no width cap and grows an arbitrary-size Python int.
    def _TCompactProtocol__readVarint(self):
        result = 0
        for index in range(10):
            byte = self.trans.readAll(1)[0]
            if index == 9 and byte > 1:
                raise ProfileError("integer_size_limit")
            result |= (byte & 0x7F) << (index * 7)
            if byte & 0x80 == 0:
                return result
        raise ProfileError("integer_size_limit")

    def _TCompactProtocol__readI16(self):
        value = self._read_zigzag()
        self._check_signed_width(value, 16)
        return value

    def readI16(self):
        value = super().readI16()
        self._check_signed_width(value, 16)
        return value

    def readI32(self):
        value = super().readI32()
        self._check_signed_width(value, 32)
        return value

    def readI64(self):
        value = super().readI64()
        self._check_signed_width(value, 64)
        return value

    def _read_zigzag(self):
        value = self._TCompactProtocol__readVarint()
        return (value >> 1) ^ -(value & 1)

    @staticmethod
    def _check_signed_width(value, bits):
        if not -(1 << (bits - 1)) <= value < (1 << (bits - 1)):
            raise ProfileError("integer_size_limit")

    def _check_string_length(self, length):
        if length > _MAX_STRING_BYTES:
            raise ProfileError("string_size_limit")

    def _check_container_length(self, length):
        if length > _MAX_CONTAINER_ITEMS:
            raise ProfileError("container_size_limit")

    def _enter(self):
        self._depth += 1
        if self._depth > _MAX_THRIFT_DEPTH:
            raise ProfileError("thrift_depth_limit")

    def _leave(self):
        self._depth -= 1

    def readStructBegin(self):
        self._enter()
        return super().readStructBegin()

    def readStructEnd(self):
        result = super().readStructEnd()
        self._leave()
        return result

    def readCollectionBegin(self):
        self._enter()
        return super().readCollectionBegin()

    readListBegin = readCollectionBegin
    readSetBegin = readCollectionBegin

    def readCollectionEnd(self):
        result = super().readCollectionEnd()
        self._leave()
        return result

    readListEnd = readCollectionEnd
    readSetEnd = readCollectionEnd

    def readMapBegin(self):
        self._enter()
        return super().readMapBegin()

    def readMapEnd(self):
        result = super().readMapEnd()
        self._leave()
        return result


def _decode_envelope(line):
    if not isinstance(line, bytes):
        raise ProfileError("invalid_envelope")
    tokens = line.split()
    if len(tokens) != 3:
        raise ProfileError("invalid_envelope")
    timestamp_token, query_id_token, payload_token = tokens
    if not timestamp_token.isdigit():
        raise ProfileError("invalid_timestamp")
    significant_timestamp = timestamp_token.lstrip(b"0") or b"0"
    if len(significant_timestamp) > 19:
        raise ProfileError("invalid_timestamp")
    logged_at_ms = int(significant_timestamp)
    if logged_at_ms > _MAX_INT64:
        raise ProfileError("invalid_timestamp")
    try:
        query_id = normalize_query_id(query_id_token.decode("ascii"))
    except UnicodeDecodeError:
        raise ProfileError("invalid_query_id") from None
    try:
        compressed = base64.b64decode(payload_token, validate=True)
    except (binascii.Error, ValueError):
        raise ProfileError("invalid_base64") from None
    return logged_at_ms, query_id, compressed


def _decompress(compressed, limit):
    if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
        raise ProfileError("invalid_size_limit")
    decompressor = zlib.decompressobj()
    try:
        decoded = decompressor.decompress(compressed, limit + 1)
        if len(decoded) > limit or decompressor.unconsumed_tail:
            raise ProfileError("decoded_size_limit")
        decoded += decompressor.flush(limit + 1 - len(decoded))
    except zlib.error:
        raise ProfileError("invalid_zlib") from None
    if len(decoded) > limit:
        raise ProfileError("decoded_size_limit")
    if not decompressor.eof or decompressor.unused_data:
        raise ProfileError("invalid_zlib")
    return decoded


def _validate_required_fields(tree):
    stack = [(tree, 1)]
    while stack:
        value, depth = stack.pop()
        if depth > _MAX_THRIFT_DEPTH:
            raise ProfileError("thrift_depth_limit")
        if isinstance(value, dict):
            stack.extend((item, depth + 1) for pair in value.items() for item in pair)
        elif isinstance(value, (list, tuple, set)):
            stack.extend((item, depth + 1) for item in value)
        elif hasattr(value, "thrift_spec") and callable(getattr(value, "validate", None)):
            value.validate()
            stack.extend((item, depth + 1) for item in value.__dict__.values() if item is not None)


def _validate_tree(tree):
    _validate_required_fields(tree)
    if not tree.nodes or len(tree.nodes) > _MAX_NODES:
        raise ProfileError("invalid_tree")
    open_slots = 1
    for node in tree.nodes:
        if not isinstance(node, TRuntimeProfileNode) or open_slots == 0:
            raise ProfileError("invalid_tree")
        if node.num_children < 0:
            raise ProfileError("invalid_tree")
        open_slots -= 1
        open_slots += node.num_children
    if open_slots != 0:
        raise ProfileError("invalid_tree")
    version = 1 if tree.profile_version is None else tree.profile_version
    if version not in (1, 2):
        raise ProfileError("unsupported_profile_version")
    _validate_event_sequences(tree)
    if version == 2:
        _validate_aggregated_nodes(tree)
    tree.profile_version = version


def _validate_event_sequences(tree):
    for node in tree.nodes:
        for sequence in node.event_sequences or ():
            if len(sequence.timestamps) != len(sequence.labels):
                raise ProfileError("invalid_event_sequence")


def _validate_aggregated_nodes(tree):
    for node in tree.nodes:
        aggregated = node.aggregated
        if aggregated is None:
            continue
        count = aggregated.num_instances
        if count is None or count <= 0:
            raise ProfileError("invalid_aggregated_profile")
        if aggregated.input_profiles is not None and len(aggregated.input_profiles) != count:
            raise ProfileError("invalid_aggregated_profile")
        for counter in aggregated.counters or ():
            _require_parallel(count, counter.has_value, counter.values)
        for counter in aggregated.summary_stats_counters or ():
            _require_parallel(
                count,
                counter.has_value,
                counter.sum,
                counter.total_num_values,
                counter.min_value,
                counter.max_value,
            )
        for counter in aggregated.time_series_counters or ():
            _require_parallel(count, counter.period_ms, counter.values, counter.start_index)
        for sequence in aggregated.event_sequences or ():
            _require_parallel(count, sequence.label_idxs, sequence.timestamps)
            for label_indexes, timestamps in zip(sequence.label_idxs, sequence.timestamps):
                if len(label_indexes) != len(timestamps):
                    raise ProfileError("invalid_aggregated_profile")
                if any(index < 0 or index >= len(sequence.label_dict) for index in label_indexes):
                    raise ProfileError("invalid_aggregated_profile")
        for values in (aggregated.info_strings or {}).values():
            for indexes in values.values():
                if any(index < 0 or index >= count for index in indexes):
                    raise ProfileError("invalid_aggregated_profile")


def _require_parallel(count, *arrays):
    if any(len(array) != count for array in arrays):
        raise ProfileError("invalid_aggregated_profile")


def _decode_thrift(decoded):
    transport = _BytesTransport(decoded)
    protocol = _BoundedCompactProtocol(transport)
    tree = TRuntimeProfileTree()
    try:
        tree.read(protocol)
        if transport.remaining:
            raise ProfileError("trailing_thrift_data")
        _validate_tree(tree)
    except ProfileError:
        raise
    except Exception:
        raise ProfileError("invalid_thrift") from None
    return tree


def decode_line(
    line: bytes, *, max_decoded_bytes: int = _DEFAULT_MAX_DECODED_BYTES
) -> DecodedProfile:
    """Decode one encoded impalad profile record."""
    logged_at_ms, query_id, compressed = _decode_envelope(line)
    decoded = _decompress(compressed, max_decoded_bytes)
    tree = _decode_thrift(decoded)
    return DecodedProfile(query_id=query_id, logged_at_ms=logged_at_ms, tree=tree)
