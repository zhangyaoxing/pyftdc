"""Low-level BSON framing and FTDC metric chunk decoding."""

from __future__ import annotations

import math
import struct
import zlib
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, cast

from bson import BSON
from bson.codec_options import CodecOptions
from bson.decimal128 import Decimal128
from bson.timestamp import Timestamp

from pyftdc.exceptions import FTDCDecodeError
from pyftdc.models import MetricValue

_UINT64_MASK = (1 << 64) - 1
_MIN_BSON_SIZE = 5
_CODEC_OPTIONS: CodecOptions[Any] = CodecOptions(tz_aware=True, tzinfo=timezone.utc)


@dataclass(frozen=True, slots=True)
class MetricSlot:
    """A compressed numeric field and its location in a reference document."""

    path: str
    initial: int
    kind: str
    part: int = 0


@dataclass(frozen=True, slots=True)
class DecodedChunk:
    """A reference document and its decoded metric rows."""

    reference: Mapping[str, Any]
    slots: tuple[MetricSlot, ...]
    rows: tuple[tuple[int, ...], ...]


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
        except Exception as exc:
            raise FTDCDecodeError(f"{source}: invalid BSON document") from exc


def decode_metric_document(document: Mapping[str, Any]) -> DecodedChunk:
    """Decode one outer FTDC document whose type is 1."""

    raw = _decompress_payload(document)
    if len(raw) < _MIN_BSON_SIZE:
        raise FTDCDecodeError("metric chunk has no reference document")

    (reference_size,) = struct.unpack_from("<I", raw)
    if reference_size < _MIN_BSON_SIZE or reference_size + 8 > len(raw):
        raise FTDCDecodeError("invalid reference document size")
    try:
        reference = BSON(raw[:reference_size]).decode(codec_options=_CODEC_OPTIONS)
    except Exception as exc:
        raise FTDCDecodeError("invalid metric reference document") from exc

    metric_count, delta_count = struct.unpack_from("<II", raw, reference_size)
    slots = tuple(_extract_slots(reference))
    if len(slots) != metric_count:
        raise FTDCDecodeError(
            f"reference contains {len(slots)} numeric slots, chunk declares {metric_count}"
        )

    encoded = memoryview(raw)[reference_size + 8 :]
    flat_deltas = _decode_deltas(encoded, metric_count * delta_count)
    current = [slot.initial for slot in slots]
    rows: list[tuple[int, ...]] = [tuple(current)]
    for sample_index in range(delta_count):
        for metric_index in range(metric_count):
            offset = metric_index * delta_count + sample_index
            current[metric_index] = (current[metric_index] + flat_deltas[offset]) & _UINT64_MASK
        rows.append(tuple(current))

    return DecodedChunk(reference, slots, tuple(rows))


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


def timestamp_for_row(chunk: DecodedChunk, row: Sequence[int]) -> datetime:
    """Return the top-level collection start time for a decoded sample."""

    for index, slot in enumerate(chunk.slots):
        if slot.path == "start" and slot.kind == "datetime":
            return datetime.fromtimestamp(_as_signed(row[index]) / 1000, tz=timezone.utc)
    raise FTDCDecodeError("metric reference document has no top-level datetime 'start'")


def _extract_slots(value: object, path: str = "") -> Iterator[MetricSlot]:
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        for name, child in mapping.items():
            child_path = f"{path}.{name}" if path else str(name)
            yield from _extract_slots(child, child_path)
        return
    if isinstance(value, (list, tuple)):
        sequence = cast(Sequence[object], value)
        for index, child in enumerate(sequence):
            child_path = f"{path}.{index}" if path else str(index)
            yield from _extract_slots(child, child_path)
        return

    if isinstance(value, bool):
        yield MetricSlot(path, int(value), "bool")
    elif isinstance(value, int):
        yield MetricSlot(path, value & _UINT64_MASK, "int")
    elif isinstance(value, float):
        number = (
            0
            if math.isnan(value)
            else max(-(1 << 63), min((1 << 63) - 1, int(value)))
        )
        yield MetricSlot(path, number & _UINT64_MASK, "float")
    elif isinstance(value, datetime):
        moment = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        yield MetricSlot(path, int(moment.timestamp() * 1000) & _UINT64_MASK, "datetime")
    elif isinstance(value, Timestamp):
        yield MetricSlot(path, value.time & _UINT64_MASK, "timestamp", 0)
        yield MetricSlot(path, value.inc & _UINT64_MASK, "timestamp", 1)
    elif isinstance(value, Decimal128):
        low, high = struct.unpack("<QQ", value.bid)
        yield MetricSlot(path, low, "decimal128", 0)
        yield MetricSlot(path, high, "decimal128", 1)


def _decode_deltas(data: memoryview, expected_count: int) -> list[int]:
    values: list[int] = []
    position = 0
    while len(values) < expected_count:
        value, position = _read_varint(data, position)
        if value:
            values.append(value)
            continue
        run_minus_one, position = _read_varint(data, position)
        run_length = run_minus_one + 1
        if len(values) + run_length > expected_count:
            raise FTDCDecodeError("zero run exceeds declared metric data")
        values.extend([0] * run_length)
    if position != len(data):
        raise FTDCDecodeError("unexpected bytes after compressed metric data")
    return values


def _read_varint(data: memoryview, position: int) -> tuple[int, int]:
    value = 0
    for shift in range(0, 70, 7):
        if position >= len(data):
            raise FTDCDecodeError("truncated varint metric data")
        byte = data[position]
        position += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            if value > _UINT64_MASK:
                raise FTDCDecodeError("varint exceeds uint64")
            return value, position
    raise FTDCDecodeError("varint exceeds uint64")


def _as_signed(value: int) -> int:
    return value if value < (1 << 63) else value - (1 << 64)
