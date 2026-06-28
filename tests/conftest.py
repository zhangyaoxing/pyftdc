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

    delta_count = len(columns[0]) if columns else 0
    assert all(len(column) == delta_count for column in columns)
    reference_bson = BSON.encode(dict(reference))
    deltas = [value for column in columns for value in column]
    compacted = _rle_zeroes(deltas)
    raw = (
        reference_bson
        + struct.pack("<II", len(columns), delta_count)
        + b"".join(_varint(value) for value in compacted)
    )
    payload = struct.pack("<I", len(raw)) + zlib.compress(raw)
    path.write_bytes(BSON.encode({"type": 1, "doc": Binary(payload)}))
    return path


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
