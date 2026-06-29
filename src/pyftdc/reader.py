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
    metric_slots,
    timestamp_for_value,
    value_for_slot,
)
from pyftdc.exceptions import FTDCDecodeError, FTDCError, MetricNotFoundError
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
        name: set[str],
        start: datetime | None = None,
        end: datetime | None = None,
        sample_rate: float = 1.0,
    ) -> dict[str, list[DataPoint]]:
        """Return sampled observations by metric name in the inclusive UTC timespan."""

        requested_names = set(name)
        if "" in requested_names:
            raise ValueError("metric names must not be empty")
        start_utc = _as_utc(start, "start") if start is not None else None
        end_utc = _as_utc(end, "end") if end is not None else None
        if start_utc is not None and end_utc is not None and start_utc > end_utc:
            raise ValueError("start must be before or equal to end")
        if not math.isfinite(sample_rate) or not 0 < sample_rate <= 1:
            raise ValueError("sample_rate must be greater than 0 and at most 1")

        found_names: set[str] = set()
        point_numbers: dict[str, int] = {}
        points_by_name: dict[str, dict[datetime, DataPoint]] = {}
        selected_names = requested_names or None
        for chunk in self._metric_chunks(start_utc, end_utc, selected_names):
            matching_slots = {
                slot.path: (index, slot)
                for index, slot in enumerate(chunk.slots)
                if not requested_names or slot.path in requested_names
            }
            if not matching_slots:
                continue
            try:
                timestamp_index = next(
                    index
                    for index, slot in enumerate(chunk.slots)
                    if slot.path == "start" and slot.kind == "datetime"
                )
            except StopIteration as exc:
                raise FTDCDecodeError(
                    "metric reference document has no top-level datetime 'start'"
                ) from exc

            found_names.update(matching_slots)
            for metric_name in matching_slots:
                point_numbers.setdefault(metric_name, 0)
                points_by_name.setdefault(metric_name, {})
            timestamps = chunk.columns[timestamp_index]
            for sample_index, raw_timestamp in enumerate(timestamps):
                timestamp = timestamp_for_value(raw_timestamp)
                if (start_utc is None or start_utc <= timestamp) and (
                    end_utc is None or timestamp <= end_utc
                ):
                    for metric_name, (metric_index, slot) in matching_slots.items():
                        point_number = point_numbers[metric_name] + 1
                        point_numbers[metric_name] = point_number
                        if int(point_number * sample_rate) == int((point_number - 1) * sample_rate):
                            continue
                        points_by_name[metric_name][timestamp] = DataPoint(
                            timestamp=timestamp,
                            value=value_for_slot(slot, chunk.columns[metric_index][sample_index]),
                        )

        missing_names = requested_names - found_names
        if missing_names:
            raise MetricNotFoundError(sorted(missing_names)[0])
        return {
            metric_name: [points[timestamp] for timestamp in sorted(points)]
            for metric_name, points in sorted(points_by_name.items())
        }

    query = get_metric

    def list_metrics(self) -> list[str]:
        """Return sorted dotted names for numeric fields in the source."""

        names: set[str] = set()
        for path in self._paths():
            with path.open("rb") as stream:
                for document in iter_bson_documents(stream, path):
                    if document.get("type") == 1:
                        names.update(slot.path for slot in metric_slots(document))
        return sorted(names)

    def _metric_chunks(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        names: set[str] | None = None,
    ) -> Iterator[DecodedChunk]:
        for path in self._paths(start, end):
            with path.open("rb") as stream:
                for document in iter_bson_documents(stream, path):
                    if document.get("type") == 1:
                        yield decode_metric_document(document, names)

    def _paths(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[Path]:
        if self.source.is_file():
            return [self.source]
        paths = sorted(
            path
            for path in self.source.glob("metrics.*")
            if path.is_file() and not path.name.endswith(".tmp")
        )
        times = {path: _time_from_filename(path) for path in paths}
        timestamped = [file_time for file_time in times.values() if file_time is not None]
        if not timestamped:
            return paths

        first_time = min(timestamped)
        lower_file_time: datetime | None = None
        if start is not None:
            preceding = [file_time for file_time in timestamped if file_time <= start]
            lower_file_time = max(preceding, default=first_time)

        upper_file_time = end
        if end is not None and end < first_time:
            upper_file_time = first_time

        return [
            path
            for path in paths
            if (file_time := times[path]) is None
            or (
                (lower_file_time is None or lower_file_time <= file_time)
                and (upper_file_time is None or file_time <= upper_file_time)
            )
        ]


def _as_utc(value: datetime, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _time_from_filename(path: Path) -> datetime | None:
    name = path.name.removeprefix("metrics.")
    timestamp, separator, sequence = name.rpartition("-")
    if not separator or not sequence.isdigit():
        return None
    try:
        return datetime.strptime(timestamp, "%Y-%m-%dT%H-%M-%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
