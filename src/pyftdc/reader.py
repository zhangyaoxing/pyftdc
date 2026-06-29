"""High-level FTDC metric query API."""

from __future__ import annotations

import math
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

from pyftdc._codec import (
    DecodedChunk,
    decode_metric_document,
    iter_bson_documents,
    timestamp_for_row,
    value_for_slot,
)
from pyftdc.exceptions import FTDCError, MetricNotFoundError
from pyftdc.models import DataPoint


class FTDCReader:
    """Read metrics from one FTDC file or a ``diagnostic.data`` directory."""

    def __init__(self, source: str | Path) -> None:
        self.source = Path(source)
        if not self.source.exists():
            raise FileNotFoundError(self.source)
        if not self.source.is_file() and not self.source.is_dir():
            raise FTDCError(f"FTDC source is not a regular file or directory: {self.source}")

    def get_metric(
        self,
        name: str,
        start: datetime,
        end: datetime,
        sample_rate: float = 1.0,
    ) -> list[DataPoint]:
        """Return sampled observations for *name* in the inclusive UTC timespan."""

        if not name:
            raise ValueError("metric name must not be empty")
        start_utc = _as_utc(start, "start")
        end_utc = _as_utc(end, "end")
        if start_utc > end_utc:
            raise ValueError("start must be before or equal to end")
        if not math.isfinite(sample_rate) or not 0 < sample_rate <= 1:
            raise ValueError("sample_rate must be greater than 0 and at most 1")

        found = False
        point_number = 0
        points_by_time: dict[datetime, DataPoint] = {}
        for chunk in self._metric_chunks():
            matching_indexes = [
                index
                for index, slot in enumerate(chunk.slots)
                if slot.path == name and slot.part == 0
            ]
            if not matching_indexes:
                continue
            found = True
            metric_index = matching_indexes[0]
            slot = chunk.slots[metric_index]
            for row in chunk.rows:
                timestamp = timestamp_for_row(chunk, row)
                if start_utc <= timestamp <= end_utc:
                    point_number += 1
                    if int(point_number * sample_rate) == int((point_number - 1) * sample_rate):
                        continue
                    points_by_time[timestamp] = DataPoint(
                        timestamp=timestamp,
                        value=value_for_slot(slot, row[metric_index]),
                    )

        if not found:
            raise MetricNotFoundError(name)
        return [points_by_time[key] for key in sorted(points_by_time)]

    query = get_metric

    def list_metrics(self) -> list[str]:
        """Return sorted dotted names for numeric fields in the source."""

        names: set[str] = set()
        for chunk in self._metric_chunks():
            names.update(slot.path for slot in chunk.slots)
        return sorted(names)

    def _metric_chunks(self) -> Iterator[DecodedChunk]:
        for path in self._paths():
            with path.open("rb") as stream:
                for document in iter_bson_documents(stream, path):
                    if document.get("type") == 1:
                        yield decode_metric_document(document)

    def _paths(self) -> list[Path]:
        if self.source.is_file():
            return [self.source]
        return sorted(
            path
            for path in self.source.glob("metrics.*")
            if path.is_file() and not path.name.endswith(".tmp")
        )


def _as_utc(value: datetime, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc)
