"""High-level FTDC metric query API."""

from __future__ import annotations

import math
import os
from collections import deque
from collections.abc import Collection, Iterator, Mapping
from concurrent.futures import Future, ProcessPoolExecutor
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from typing import Any, cast

from pyftdc._codec import (
    DecodedChunk,
    decode_metric_document,
    iter_bson_documents,
    metric_slots,
    peek_chunk_timespan,
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

    def get_metric(  # pylint: disable=too-many-arguments
        self,
        name: set[str],
        start: datetime | None = None,
        end: datetime | None = None,
        sample_rate: float = 1.0,
        sort_by_timestamp: bool = False,
        *,
        workers: int | None = None,
    ) -> dict[str, list[DataPoint]]:
        """Return sampled observations by metric name in the inclusive UTC timespan.

        Points retain source traversal order unless ``sort_by_timestamp`` is true.
        Metric chunks are decoded by ``workers`` processes. By default, the worker
        count is one less than the number of CPUs, with a minimum of one.
        """

        requested_names = set(name)
        start_utc, end_utc, worker_count = _query_options(
            requested_names, start, end, sample_rate, workers
        )

        found_names: set[str] = set()
        point_numbers: dict[str, int] = {}
        points_by_name: dict[str, dict[datetime, DataPoint]] = {}
        selected_names = requested_names or None
        time_bounded = start_utc is not None or end_utc is not None
        do_sample = sample_rate < 1.0
        for chunk in self._metric_chunks(start_utc, end_utc, selected_names, worker_count):
            matching_slots = {
                slot.path: (index, slot)
                for index, slot in enumerate(chunk.slots)
                if not requested_names or slot.path in requested_names
            }
            if not matching_slots:
                continue
            timestamp_index = _timestamp_index(chunk)

            found_names.update(matching_slots)
            for metric_name in matching_slots:
                if metric_name not in point_numbers:
                    point_numbers[metric_name] = 0
                    points_by_name[metric_name] = {}
            timestamps = chunk.columns[timestamp_index]
            for sample_index, raw_timestamp in enumerate(timestamps):
                timestamp = timestamp_for_value(raw_timestamp)
                if time_bounded and (
                    (start_utc is not None and timestamp < start_utc)
                    or (end_utc is not None and timestamp > end_utc)
                ):
                    continue
                if do_sample:
                    for metric_name, (metric_index, slot) in matching_slots.items():
                        point_number = point_numbers[metric_name] + 1
                        point_numbers[metric_name] = point_number
                        if int(point_number * sample_rate) == int((point_number - 1) * sample_rate):
                            continue
                        points_by_name[metric_name][timestamp] = DataPoint(
                            timestamp=timestamp,
                            value=value_for_slot(slot, chunk.columns[metric_index][sample_index]),
                        )
                else:
                    for metric_name, (metric_index, slot) in matching_slots.items():
                        points_by_name[metric_name][timestamp] = DataPoint(
                            timestamp=timestamp,
                            value=value_for_slot(slot, chunk.columns[metric_index][sample_index]),
                        )

        missing_names = requested_names - found_names
        if missing_names:
            raise MetricNotFoundError(sorted(missing_names)[0])
        return {
            metric_name: [
                points[timestamp] for timestamp in (sorted(points) if sort_by_timestamp else points)
            ]
            for metric_name, points in sorted(points_by_name.items())
        }

    query = get_metric

    def get_metadata(self) -> dict[str, Any]:
        """Return the complete metadata payload from the first source file."""

        paths = self._paths()
        if paths:
            path = paths[0]
            with path.open("rb") as stream:
                for document in iter_bson_documents(stream, path):
                    metadata = document.get("doc")
                    if document.get("type") != 0 or not isinstance(metadata, Mapping):
                        continue
                    return dict(cast(Mapping[str, Any], metadata))
        raise FTDCError("MongoDB metadata not found in FTDC source")

    def get_mongodb_config(self) -> dict[str, Any]:
        """Return the parsed MongoDB command-line configuration."""

        command_line_options = self.get_metadata().get("getCmdLineOpts")
        if isinstance(command_line_options, Mapping):
            typed_options = cast(Mapping[str, Any], command_line_options)
            parsed = typed_options.get("parsed")
            if isinstance(parsed, Mapping):
                return dict(cast(Mapping[str, Any], parsed))
        raise FTDCError("MongoDB configuration not found in FTDC source")

    def get_build_info(self) -> dict[str, Any]:
        """Return MongoDB build and version metadata."""

        return self._get_metadata_mapping("buildInfo", "build information")

    def get_host_info(self) -> dict[str, Any]:
        """Return host operating-system and hardware metadata."""

        return self._get_metadata_mapping("hostInfo", "host information")

    def get_ulimits(self) -> dict[str, Any]:
        """Return process resource-limit metadata."""

        return self._get_metadata_mapping("ulimits", "ulimits")

    def get_sys_max_open_files(self) -> dict[str, Any]:
        """Return system-wide maximum open-file metadata."""

        return self._get_metadata_mapping("sysMaxOpenFiles", "maximum open files")

    def get_metadata_start(self) -> datetime:
        """Return the UTC timestamp at which metadata collection started."""

        return self._get_metadata_timestamp("start")

    def get_metadata_end(self) -> datetime:
        """Return the UTC timestamp at which metadata collection ended."""

        return self._get_metadata_timestamp("end")

    def list_metrics(self, *, all_chunks: bool = False) -> list[str]:
        """Return sorted dotted names for numeric fields in the source.

        By default, inspect only the first metric chunk. Set ``all_chunks`` to
        scan the complete source and include metrics introduced by later chunks.
        """

        names: set[str] = set()
        for path in self._paths():
            with path.open("rb") as stream:
                for document in iter_bson_documents(stream, path):
                    if document.get("type") == 1:
                        names.update(slot.path for slot in metric_slots(document))
                        if not all_chunks:
                            return sorted(names)
        return sorted(names)

    def _get_metadata_mapping(self, key: str, label: str) -> dict[str, Any]:
        value = self.get_metadata().get(key)
        if isinstance(value, Mapping):
            return dict(cast(Mapping[str, Any], value))
        raise FTDCError(f"MongoDB {label} metadata not found in FTDC source")

    def _get_metadata_timestamp(self, key: str) -> datetime:
        value = self.get_metadata().get(key)
        if isinstance(value, datetime):
            return value
        raise FTDCError(f"MongoDB metadata {key} timestamp not found in FTDC source")

    def _metric_chunks(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        names: set[str] | None = None,
        workers: int = 1,
    ) -> Iterator[DecodedChunk]:
        documents = self._metric_documents(start, end)

        def _in_range(document: Mapping[str, Any]) -> bool:
            if start is None and end is None:
                return True
            timespan = peek_chunk_timespan(document)
            if timespan is None:
                return True
            chunk_start, sample_count = timespan
            if end is not None and chunk_start > end:
                return False
            return start is None or chunk_start.timestamp() + sample_count >= start.timestamp()

        in_range_docs = [doc for doc in documents if _in_range(doc)]

        if workers == 1 or len(in_range_docs) < 2:
            for doc in in_range_docs:
                yield decode_metric_document(doc, names)
            return

        effective_workers = min(workers, len(in_range_docs))
        with ProcessPoolExecutor(max_workers=effective_workers) as executor:
            pending: deque[Future[DecodedChunk]] = deque(
                executor.submit(_decode_metric_document, doc, names)
                for doc in islice(in_range_docs, effective_workers)
            )
            for doc in islice(in_range_docs, effective_workers, None):
                yield pending.popleft().result()
                pending.append(executor.submit(_decode_metric_document, doc, names))
            while pending:
                yield pending.popleft().result()

    def _metric_documents(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        for path in self._paths(start, end):
            with path.open("rb") as stream:
                for document in iter_bson_documents(stream, path):
                    if document.get("type") == 1:
                        yield document

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


def _query_options(
    requested_names: set[str],
    start: datetime | None,
    end: datetime | None,
    sample_rate: float,
    workers: object,
) -> tuple[datetime | None, datetime | None, int]:
    if "" in requested_names:
        raise ValueError("metric names must not be empty")
    start_utc = _as_utc(start, "start") if start is not None else None
    end_utc = _as_utc(end, "end") if end is not None else None
    if start_utc is not None and end_utc is not None and start_utc > end_utc:
        raise ValueError("start must be before or equal to end")
    if not math.isfinite(sample_rate) or not 0 < sample_rate <= 1:
        raise ValueError("sample_rate must be greater than 0 and at most 1")
    return start_utc, end_utc, _worker_count(workers)


def _timestamp_index(chunk: DecodedChunk) -> int:
    try:
        return next(
            index
            for index, slot in enumerate(chunk.slots)
            if slot.path == "start" and slot.kind == "datetime"
        )
    except StopIteration as exc:
        raise FTDCDecodeError(
            "metric reference document has no top-level datetime 'start'"
        ) from exc


def _worker_count(workers: object = None) -> int:
    if workers is None:
        return max(1, (os.cpu_count() or 1) - 1)
    if isinstance(workers, bool) or not isinstance(workers, int) or workers < 1:
        raise ValueError("workers must be a positive integer")
    return workers


def _decode_metric_document(
    document: Mapping[str, Any],
    names: Collection[str] | None,
) -> DecodedChunk:
    """Decode a metric document in a worker process."""

    return decode_metric_document(document, names)


def _time_from_filename(path: Path) -> datetime | None:
    name = path.name.removeprefix("metrics.")
    timestamp, separator, sequence = name.rpartition("-")
    if not separator or not sequence.isdigit():
        return None
    try:
        return datetime.strptime(timestamp, "%Y-%m-%dT%H-%M-%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
