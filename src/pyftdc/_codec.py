"""Low-level BSON framing and FTDC metric chunk decoding."""

from __future__ import annotations

import math
import struct
import zlib
from collections.abc import Collection, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO

from bson import BSON
from bson.codec_options import CodecOptions
from bson.errors import InvalidBSON

from pyftdc.exceptions import FTDCDecodeError
from pyftdc.models import MetricValue

_UINT64_MASK = (1 << 64) - 1
_MIN_BSON_SIZE = 5
_CODEC_OPTIONS: CodecOptions[Any] = CodecOptions(tz_aware=True, tzinfo=timezone.utc)

# BSON element type bytes, keyed by the encoding used in reference documents.
_ELEMENT_DOUBLE = 0x01
_ELEMENT_STRING = 0x02
_ELEMENT_DOCUMENT = 0x03
_ELEMENT_ARRAY = 0x04
_ELEMENT_BINARY = 0x05
_ELEMENT_UNDEFINED = 0x06
_ELEMENT_OBJECT_ID = 0x07
_ELEMENT_BOOLEAN = 0x08
_ELEMENT_DATETIME = 0x09
_ELEMENT_NULL = 0x0A
_ELEMENT_REGEX = 0x0B
_ELEMENT_DB_POINTER = 0x0C
_ELEMENT_CODE = 0x0D
_ELEMENT_SYMBOL = 0x0E
_ELEMENT_CODE_W_SCOPE = 0x0F
_ELEMENT_INT32 = 0x10
_ELEMENT_TIMESTAMP = 0x11
_ELEMENT_INT64 = 0x12
_ELEMENT_DECIMAL128 = 0x13
_ELEMENT_MAX_KEY = 0x7F
_ELEMENT_MIN_KEY = 0xFF

# Element types whose value has a fixed width, including the empty ones.
_FIXED_ELEMENT_SIZES = {
    _ELEMENT_DOUBLE: 8,
    _ELEMENT_UNDEFINED: 0,
    _ELEMENT_OBJECT_ID: 12,
    _ELEMENT_BOOLEAN: 1,
    _ELEMENT_DATETIME: 8,
    _ELEMENT_NULL: 0,
    _ELEMENT_INT32: 4,
    _ELEMENT_TIMESTAMP: 8,
    _ELEMENT_INT64: 8,
    _ELEMENT_DECIMAL128: 16,
    _ELEMENT_MAX_KEY: 0,
    _ELEMENT_MIN_KEY: 0,
}


@dataclass(frozen=True, slots=True)
class MetricSlot:
    """A compressed numeric field and its location in a reference document."""

    path: str
    initial: int
    kind: str
    part: int = 0


@dataclass(frozen=True, slots=True)
class DecodedChunk:
    """Selected metric slots and their decoded sample columns."""

    slots: tuple[MetricSlot, ...]
    columns: tuple[list[int], ...]


def iter_bson_documents(stream: BinaryIO, source: Path) -> Iterator[Mapping[str, Any]]:
    """Yield the concatenated BSON documents in an FTDC file."""

    while prefix := stream.read(4):
        if len(prefix) != 4:
            raise FTDCDecodeError(f"{source}: truncated BSON length")
        (length,) = struct.unpack("<I", prefix)
        if length == 0:  # Zero bytes terminate an interim file.
            return
        if length < _MIN_BSON_SIZE:
            raise FTDCDecodeError(f"{source}: invalid BSON length {length}")
        remainder = stream.read(length - 4)
        if len(remainder) != length - 4:
            raise FTDCDecodeError(f"{source}: truncated BSON document")
        try:
            yield BSON(prefix + remainder).decode(codec_options=_CODEC_OPTIONS)
        except InvalidBSON as exc:
            raise FTDCDecodeError(f"{source}: invalid BSON document") from exc


def decode_metric_document(
    document: Mapping[str, Any],
    names: Collection[str] | None = None,
) -> DecodedChunk:
    """Decode one outer FTDC document whose type is 1."""

    slots, delta_count, encoded = _metric_parts(document)
    metric_count = len(slots)
    selected_indices = [
        index
        for index, slot in enumerate(slots)
        if slot.part == 0 and (names is None or slot.path == "start" or slot.path in names)
    ]
    selected_slots = tuple(slots[index] for index in selected_indices)
    deltas = _decode_columns(encoded, metric_count, delta_count, selected_indices)
    columns = tuple(
        _accumulate_column(slot.initial, column)
        for slot, column in zip(selected_slots, deltas, strict=True)
    )
    return DecodedChunk(selected_slots, columns)


def metric_slots(document: Mapping[str, Any]) -> tuple[MetricSlot, ...]:
    """Return numeric field metadata without expanding the metric samples."""

    slots, _, _ = _metric_parts(document)
    return slots


def peek_chunk_timespan(
    document: Mapping[str, Any],
) -> tuple[datetime, int] | None:
    """Return (start_timestamp, sample_count) without decoding metric columns.

    Returns None when the document is not a metric chunk or has no start field.
    """

    compressed = _compressed_payload(document)
    if compressed is None:
        return None
    expected_size, data = compressed
    try:
        raw = zlib.decompress(data)
    except zlib.error:
        return None
    if len(raw) != expected_size or len(raw) < _MIN_BSON_SIZE:
        return None

    reference_parts = _peek_reference(raw)
    if reference_parts is None:
        return None
    reference, reference_size = reference_parts
    start = reference.get("start")
    if not isinstance(start, datetime):
        return None
    delta_count = struct.unpack_from("<I", raw, reference_size + 4)[0]
    return start, delta_count


def _compressed_payload(
    document: Mapping[str, Any],
) -> tuple[int, bytes] | None:
    payload = document.get("data", document.get("doc"))
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        return None
    data = payload.tobytes() if isinstance(payload, memoryview) else bytes(payload)
    if len(data) < 5:
        return None
    return struct.unpack_from("<I", data)[0], data[4:]


def _peek_reference(raw: bytes) -> tuple[Mapping[str, Any], int] | None:
    (reference_size,) = struct.unpack_from("<I", raw)
    if reference_size < _MIN_BSON_SIZE or reference_size + 8 > len(raw):
        return None
    try:
        reference = BSON(raw[:reference_size]).decode(codec_options=_CODEC_OPTIONS)
    except InvalidBSON:
        return None
    return reference, reference_size


def _metric_parts(
    document: Mapping[str, Any],
) -> tuple[tuple[MetricSlot, ...], int, memoryview]:
    raw = _decompress_payload(document)
    if len(raw) < _MIN_BSON_SIZE:
        raise FTDCDecodeError("metric chunk has no reference document")

    (reference_size,) = struct.unpack_from("<I", raw)
    if reference_size < _MIN_BSON_SIZE or reference_size + 8 > len(raw):
        raise FTDCDecodeError("invalid reference document size")

    metric_count, delta_count = struct.unpack_from("<II", raw, reference_size)
    slots = tuple(_reference_slots(raw, reference_size))
    if len(slots) != metric_count:
        raise FTDCDecodeError(
            f"reference contains {len(slots)} numeric slots, chunk declares {metric_count}"
        )

    encoded = memoryview(raw)[reference_size + 8 :]
    return slots, delta_count, encoded


def _decompress_payload(document: Mapping[str, Any]) -> bytes:
    """Validate and decompress the binary payload of a metric document."""

    payload = document.get("data", document.get("doc"))
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise FTDCDecodeError("metric document has no binary 'data' or 'doc' field")
    data = payload.tobytes() if isinstance(payload, memoryview) else bytes(payload)
    if len(data) < 5:
        raise FTDCDecodeError("compressed metric chunk is too short")

    (expected_size,) = struct.unpack_from("<I", data)
    try:
        raw = zlib.decompress(data[4:])
    except zlib.error as exc:
        raise FTDCDecodeError("invalid zlib metric payload") from exc
    if len(raw) != expected_size:
        raise FTDCDecodeError(
            f"metric chunk size mismatch: expected {expected_size}, got {len(raw)}"
        )
    return raw


def value_for_slot(slot: MetricSlot, raw_value: int) -> MetricValue:
    """Restore a compressed integer to the reference field's useful Python type."""

    signed = _as_signed(raw_value)
    if slot.kind == "bool":
        return bool(raw_value)
    if slot.kind == "float":
        return float(signed)
    return signed


def timestamp_for_value(raw_value: int) -> datetime:
    """Restore a compressed top-level start value to a UTC timestamp."""

    return datetime.fromtimestamp(_as_signed(raw_value) / 1000, tz=timezone.utc)


def _reference_slots(raw: bytes, size: int) -> Iterator[MetricSlot]:
    """Yield reference slots in document order, duplicate field names included.

    BSON allows repeated field names, and MongoDB's ``systemMetrics.mounts``
    does repeat them when one mount point is mounted twice (for example two
    ``/boot/efi`` entries). Decoding the reference into a mapping keeps only
    the last value, which loses delta columns and shifts every following
    metric index. The delta stream addresses each encoded value, so the
    reference is walked as raw BSON instead.
    """

    yield from _walk_elements(raw, 4, size, "")


def _walk_elements(raw: bytes, position: int, end: int, path: str) -> Iterator[MetricSlot]:
    while position < end - 1:
        type_byte = raw[position]
        position += 1
        name, position = _read_cstring(raw, position, end)
        child_path = f"{path}.{name}" if path else name
        if type_byte in (_ELEMENT_DOCUMENT, _ELEMENT_ARRAY):
            (document_size,) = struct.unpack_from("<i", raw, position)
            document_end = position + document_size
            if document_size < _MIN_BSON_SIZE or document_end > end:
                raise FTDCDecodeError("invalid nested document in metric reference")
            yield from _walk_elements(raw, position + 4, document_end, child_path)
            position = document_end
            continue
        yield from _element_slots(raw, position, child_path, type_byte)
        position = _skip_element(raw, position, type_byte)
        if position > end:
            raise FTDCDecodeError("truncated metric reference document")


def _read_cstring(raw: bytes, position: int, end: int) -> tuple[str, int]:
    terminator = raw.find(b"\x00", position, end)
    if terminator < 0:
        raise FTDCDecodeError("unterminated element name in metric reference")
    return raw[position:terminator].decode("utf-8", "surrogatepass"), terminator + 1


def _element_slots(
    raw: bytes,
    position: int,
    path: str,
    type_byte: int,
) -> Iterator[MetricSlot]:
    if type_byte == _ELEMENT_BOOLEAN:
        yield MetricSlot(path, int(raw[position]), "bool")
    elif type_byte == _ELEMENT_INT32:
        (value,) = struct.unpack_from("<i", raw, position)
        yield MetricSlot(path, value & _UINT64_MASK, "int")
    elif type_byte == _ELEMENT_INT64:
        (value,) = struct.unpack_from("<q", raw, position)
        yield MetricSlot(path, value & _UINT64_MASK, "int")
    elif type_byte == _ELEMENT_DOUBLE:
        (value,) = struct.unpack_from("<d", raw, position)
        number = 0 if math.isnan(value) else max(-(1 << 63), min((1 << 63) - 1, int(value)))
        yield MetricSlot(path, number & _UINT64_MASK, "float")
    elif type_byte == _ELEMENT_DATETIME:
        (value,) = struct.unpack_from("<q", raw, position)
        yield MetricSlot(path, value & _UINT64_MASK, "datetime")
    elif type_byte == _ELEMENT_TIMESTAMP:
        increment, seconds = struct.unpack_from("<II", raw, position)
        yield MetricSlot(path, seconds & _UINT64_MASK, "timestamp", 0)
        yield MetricSlot(path, increment & _UINT64_MASK, "timestamp", 1)
    elif type_byte == _ELEMENT_DECIMAL128:
        low, high = struct.unpack_from("<QQ", raw, position)
        yield MetricSlot(path, low, "decimal128", 0)
        yield MetricSlot(path, high, "decimal128", 1)


def _skip_element(raw: bytes, position: int, type_byte: int) -> int:
    if (fixed_size := _FIXED_ELEMENT_SIZES.get(type_byte)) is not None:
        return position + fixed_size
    if type_byte in (_ELEMENT_STRING, _ELEMENT_CODE, _ELEMENT_SYMBOL):
        (length,) = struct.unpack_from("<i", raw, position)
        return position + 4 + length
    if type_byte == _ELEMENT_BINARY:
        (length,) = struct.unpack_from("<i", raw, position)
        return position + 5 + length
    if type_byte == _ELEMENT_DB_POINTER:
        (length,) = struct.unpack_from("<i", raw, position)
        return position + 4 + length + 12
    if type_byte == _ELEMENT_CODE_W_SCOPE:
        (length,) = struct.unpack_from("<i", raw, position)
        return position + length
    if type_byte == _ELEMENT_REGEX:
        _, position = _read_cstring(raw, position, len(raw))
        _, position = _read_cstring(raw, position, len(raw))
        return position
    raise FTDCDecodeError(f"unsupported BSON element type 0x{type_byte:02x} in metric reference")


def _decode_columns(
    data: memoryview,
    metric_count: int,
    delta_count: int,
    selected_indices: list[int],
) -> tuple[list[int], ...]:
    """Decode selected columns while validating the complete delta stream."""

    expected_count = metric_count * delta_count
    selected: dict[int, list[int]] = {index: [] for index in selected_indices}
    position = 0
    logical_position = 0
    while logical_position < expected_count:
        value, position = _read_varint(data, position)
        if value:
            metric_index = logical_position // delta_count if delta_count else 0
            if metric_index in selected:
                selected[metric_index].append(value)
            logical_position += 1
            continue

        run_minus_one, position = _read_varint(data, position)
        run_length = run_minus_one + 1
        run_end = logical_position + run_length
        if run_end > expected_count:
            raise FTDCDecodeError("zero run exceeds declared metric data")
        _append_zero_run(selected, logical_position, run_end, delta_count)
        logical_position = run_end
    if position != len(data):
        raise FTDCDecodeError("unexpected bytes after compressed metric data")
    if any(len(column) != delta_count for column in selected.values()):
        raise FTDCDecodeError("invalid selected metric column length")
    return tuple(selected[index] for index in selected_indices)


def _read_varint(data: memoryview, position: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while True:
        if position >= len(data):
            raise FTDCDecodeError("truncated varint metric data")
        byte = data[position]
        position += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            if value > _UINT64_MASK:
                raise FTDCDecodeError("varint exceeds uint64")
            return value, position
        shift += 7
        if shift >= 70:
            raise FTDCDecodeError("varint exceeds uint64")


def _append_zero_run(
    selected: Mapping[int, list[int]],
    run_start: int,
    run_end: int,
    delta_count: int,
) -> None:
    if not delta_count:
        return
    first_metric = run_start // delta_count
    last_metric = (run_end - 1) // delta_count
    for metric_index in range(first_metric, last_metric + 1):
        column = selected.get(metric_index)
        if column is None:
            continue
        column_start = metric_index * delta_count
        overlap_start = max(run_start, column_start)
        overlap_end = min(run_end, column_start + delta_count)
        column.extend([0] * (overlap_end - overlap_start))


def _accumulate_column(initial: int, deltas: list[int]) -> list[int]:
    n = len(deltas)
    values = [0] * (n + 1)
    current = initial
    values[0] = current
    for i, delta in enumerate(deltas, 1):
        current = (current + delta) & _UINT64_MASK
        values[i] = current
    return values


def _as_signed(value: int) -> int:
    return value if value < (1 << 63) else value - (1 << 64)
