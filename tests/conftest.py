"""Small valid FTDC fixture builders for unit tests."""

import struct
import zlib
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from bson import BSON, Binary


def write_ftdc(
    path: Path,
    reference: Mapping[str, Any],
    columns: Sequence[Sequence[int]],
) -> Path:
    """Write one type-1 chunk. Columns contain deltas after the reference."""

    reference_bson = BSON.encode(dict(reference))
    return write_ftdc_reference(path, reference_bson, len(columns), columns)


def write_ftdc_reference(
    path: Path,
    reference_bson: bytes,
    metric_count: int,
    columns: Sequence[Sequence[int]],
) -> Path:
    """Write one type-1 chunk from pre-encoded reference bytes.

    Tests that need a reference BSON a mapping cannot express, such as a
    document with repeated field names, build the bytes directly.
    """

    delta_count = len(columns[0]) if columns else 0
    assert all(len(column) == delta_count for column in columns)
    deltas = [value for column in columns for value in column]
    compacted = _rle_zeroes(deltas)
    raw = (
        reference_bson
        + struct.pack("<II", metric_count, delta_count)
        + b"".join(_varint(value) for value in compacted)
    )
    payload = struct.pack("<I", len(raw)) + zlib.compress(raw)
    path.write_bytes(BSON.encode({"type": 1, "data": Binary(payload)}))
    return path


def bson_document(*elements: bytes) -> bytes:
    """Encode one BSON document from already-encoded elements."""

    body = b"".join(elements)
    return struct.pack("<i", len(body) + 5) + body + b"\x00"


def bson_int32(name: str, value: int) -> bytes:
    """Encode an int32 element."""

    return b"\x10" + name.encode() + b"\x00" + struct.pack("<i", value)


def bson_int64(name: str, value: int) -> bytes:
    """Encode an int64 element."""

    return b"\x12" + name.encode() + b"\x00" + struct.pack("<q", value)


def bson_datetime(name: str, milliseconds: int) -> bytes:
    """Encode a UTC datetime element from epoch milliseconds."""

    return b"\x09" + name.encode() + b"\x00" + struct.pack("<q", milliseconds)


def bson_timestamp(name: str, seconds: int, increment: int) -> bytes:
    """Encode a BSON timestamp element."""

    return b"\x11" + name.encode() + b"\x00" + struct.pack("<II", increment, seconds)


def bson_embedded_document(name: str, *elements: bytes) -> bytes:
    """Encode a nested document element."""

    return b"\x03" + name.encode() + b"\x00" + bson_document(*elements)


def _rle_zeroes(values: Sequence[int]) -> list[int]:
    output: list[int] = []
    zeroes = 0
    for value in values:
        if value == 0:
            zeroes += 1
        else:
            if zeroes:
                output.extend((0, zeroes - 1))
                zeroes = 0
            output.append(value & ((1 << 64) - 1))
    if zeroes:
        output.extend((0, zeroes - 1))
    return output


def _varint(value: int) -> bytes:
    output = bytearray()
    while value > 0x7F:
        output.append((value & 0x7F) | 0x80)
        value >>= 7
    output.append(value)
    return bytes(output)
