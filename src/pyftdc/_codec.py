"""Low-level BSON framing and FTDC metric chunk decoding."""

from __future__ import annotations

import math
import struct
import zlib
from collections.abc import Collection, Iterator, Mapping, Sequence
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
    """A reference document and its selected metric columns."""

    reference: Mapping[str, Any]
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
        except Exception as exc:
            raise FTDCDecodeError(f"{source}: invalid BSON document") from exc


def decode_metric_document(
    document: Mapping[str, Any],
    names: Collection[str] | None = None,
) -> DecodedChunk:
    """Decode one outer FTDC document whose type is 1."""

    reference, slots, delta_count, encoded = _metric_parts(document)
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
    return DecodedChunk(reference, selected_slots, columns)


def metric_slots(document: Mapping[str, Any]) -> tuple[MetricSlot, ...]:
    """Return numeric field metadata without expanding the metric samples."""

    _, slots, _, _ = _metric_parts(document)
    return slots


def peek_chunk_timespan(
    document: Mapping[str, Any],
) -> tuple[datetime, int] | None:
    """Return (start_timestamp, sample_count) without decoding metric columns.

    Returns None when the document is not a metric chunk or has no start field.
    """

    payload = document.get("data", document.get("doc"))
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        return None
    data = payload.tobytes() if isinstance(payload, memoryview) else bytes(payload)
    if len(data) < 5:
        return None
    (expected_size,) = struct.unpack_from("<I", data)
    try:
        raw = zlib.decompress(data[4:])
    except zlib.error:
        return None
    if len(raw) != expected_size or len(raw) < _MIN_BSON_SIZE:
        return None
    (reference_size,) = struct.unpack_from("<I", raw)
    if reference_size < _MIN_BSON_SIZE or reference_size + 8 > len(raw):
        return None
    try:
        reference = BSON(raw[:reference_size]).decode(codec_options=_CODEC_OPTIONS)
    except Exception:
        return None
    start = reference.get("start")
    if not isinstance(start, datetime):
        return None
    delta_count = struct.unpack_from("<I", raw, reference_size + 4)[0]
    return start, delta_count


def _metric_parts(
    document: Mapping[str, Any],
) -> tuple[Mapping[str, Any], tuple[MetricSlot, ...], int, memoryview]:
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
    return reference, slots, delta_count, encoded


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
        number = 0 if math.isnan(value) else max(-(1 << 63), min((1 << 63) - 1, int(value)))
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
    data_length = len(data)
    while logical_position < expected_count:
        value = 0
        shift = 0
        while True:
            if position >= data_length:
                raise FTDCDecodeError("truncated varint metric data")
            byte = data[position]
            position += 1
            value |= (byte & 0x7F) << shift
            if not byte & 0x80:
                if value > _UINT64_MASK:
                    raise FTDCDecodeError("varint exceeds uint64")
                break
            shift += 7
            if shift >= 70:
                raise FTDCDecodeError("varint exceeds uint64")

        if value:
            metric_index = logical_position // delta_count if delta_count else 0
            if metric_index in selected:
                selected[metric_index].append(value)
            logical_position += 1
            continue

        run_minus_one = 0
        shift = 0
        while True:
            if position >= data_length:
                raise FTDCDecodeError("truncated varint metric data")
            byte = data[position]
            position += 1
            run_minus_one |= (byte & 0x7F) << shift
            if not byte & 0x80:
                if run_minus_one > _UINT64_MASK:
                    raise FTDCDecodeError("varint exceeds uint64")
                break
            shift += 7
            if shift >= 70:
                raise FTDCDecodeError("varint exceeds uint64")

        run_length = run_minus_one + 1
        run_end = logical_position + run_length
        if run_end > expected_count:
            raise FTDCDecodeError("zero run exceeds declared metric data")
        if delta_count:
            first_metric = logical_position // delta_count
            last_metric = (run_end - 1) // delta_count
            for metric_index in range(first_metric, last_metric + 1):
                column = selected.get(metric_index)
                if column is None:
                    continue
                column_start = metric_index * delta_count
                overlap_start = max(logical_position, column_start)
                overlap_end = min(run_end, column_start + delta_count)
                column.extend([0] * (overlap_end - overlap_start))
        logical_position = run_end
    if position != data_length:
        raise FTDCDecodeError("unexpected bytes after compressed metric data")
    if any(len(column) != delta_count for column in selected.values()):
        raise FTDCDecodeError("invalid selected metric column length")
    return tuple(selected[index] for index in selected_indices)


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
