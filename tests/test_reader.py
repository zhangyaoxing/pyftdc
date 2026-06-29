"""Tests for the public FTDC reader API."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pyftdc import FTDCReader, MetricNotFoundError
from tests.conftest import write_ftdc


def test_get_metric_filters_timespan_and_decodes_deltas(tmp_path: Path) -> None:
    """Metric queries decode deltas and apply inclusive time filtering."""

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    write_ftdc(
        tmp_path / "metrics.2026-01-01T00-00-00Z-00000",
        {"start": start, "serverStatus": {"connections": {"current": 10}}},
        [[1000, 1000, 1000], [2, 0, (1 << 64) - 5]],
    )

    result = FTDCReader(tmp_path).get_metric(
        {"serverStatus.connections.current"},
        start + timedelta(seconds=1),
        start + timedelta(seconds=2),
    )
    points = result["serverStatus.connections.current"]

    assert [point.timestamp for point in points] == [
        start + timedelta(seconds=1),
        start + timedelta(seconds=2),
    ]
    assert [point.value for point in points] == [12, 12]


def test_get_metric_returns_multiple_metrics(tmp_path: Path) -> None:
    """A query returns a separate point list for each requested metric."""

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    path = write_ftdc(
        tmp_path / "metrics.interim",
        {"start": start, "value": 1, "other": 10},
        [[1000, 1000], [1, 1], [2, 2]],
    )

    result = FTDCReader(path).get_metric({"value", "other"}, start, start + timedelta(seconds=2))

    assert list(result) == ["other", "value"]
    assert [point.value for point in result["value"]] == [1, 2, 3]
    assert [point.value for point in result["other"]] == [10, 12, 14]


def test_get_metric_optionally_sorts_points_by_timestamp(tmp_path: Path) -> None:
    """Points retain source order by default and can be sorted on request."""

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    path = write_ftdc(
        tmp_path / "metrics.interim",
        {"start": start, "value": 0},
        [[2000, (1 << 64) - 1000], [1, 1]],
    )
    reader = FTDCReader(path)

    source_order = reader.get_metric({"value"})["value"]
    sorted_order = reader.get_metric({"value"}, sort_by_timestamp=True)["value"]

    assert [point.timestamp for point in source_order] == [
        start,
        start + timedelta(seconds=2),
        start + timedelta(seconds=1),
    ]
    assert [(point.timestamp, point.value) for point in sorted_order] == [
        (start, 0),
        (start + timedelta(seconds=1), 2),
        (start + timedelta(seconds=2), 1),
    ]


def test_omitted_timespan_reads_earliest_through_latest_in_folder(tmp_path: Path) -> None:
    """Omitted bounds include every sample across all files in the folder."""

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    write_ftdc(tmp_path / "metrics.1", {"start": start, "value": 1}, [[], []])
    write_ftdc(
        tmp_path / "metrics.2",
        {"start": start + timedelta(hours=1), "value": 2},
        [[], []],
    )

    result = FTDCReader(tmp_path).get_metric({"value"})

    assert [point.timestamp for point in result["value"]] == [
        start,
        start + timedelta(hours=1),
    ]
    assert [point.value for point in result["value"]] == [1, 2]


def test_each_timespan_bound_can_be_omitted(tmp_path: Path) -> None:
    """Either omitted bound expands to the corresponding archive endpoint."""

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    path = write_ftdc(
        tmp_path / "metrics.interim",
        {"start": start, "value": 0},
        [[1000, 1000], [1, 1]],
    )
    reader = FTDCReader(path)

    through_middle = reader.get_metric({"value"}, end=start + timedelta(seconds=1))
    from_middle = reader.get_metric({"value"}, start=start + timedelta(seconds=1))

    assert [point.value for point in through_middle["value"]] == [0, 1]
    assert [point.value for point in from_middle["value"]] == [1, 2]


def test_start_bound_skips_older_timestamped_files(tmp_path: Path) -> None:
    """Files before the start candidate are not opened."""

    start = datetime(2026, 1, 2, tzinfo=timezone.utc)
    (tmp_path / "metrics.2026-01-01T00-00-00Z-00000").write_bytes(b"not BSON")
    write_ftdc(
        tmp_path / "metrics.2026-01-02T00-00-00Z-00000",
        {"start": start, "value": 1},
        [[], []],
    )

    result = FTDCReader(tmp_path).get_metric({"value"}, start=start)

    assert [point.value for point in result["value"]] == [1]


def test_end_bound_skips_newer_timestamped_files(tmp_path: Path) -> None:
    """Files starting after the end bound are not opened."""

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    write_ftdc(
        tmp_path / "metrics.2026-01-01T00-00-00Z-00000",
        {"start": start, "value": 1},
        [[], []],
    )
    (tmp_path / "metrics.2026-01-02T00-00-00Z-00000").write_bytes(b"not BSON")

    result = FTDCReader(tmp_path).get_metric({"value"}, end=start)

    assert [point.value for point in result["value"]] == [1]


def test_empty_names_returns_all_metrics(tmp_path: Path) -> None:
    """An empty name set selects every metric in the archive."""

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    path = write_ftdc(
        tmp_path / "metrics.interim",
        {"start": start, "value": 1, "other": 10},
        [[1000], [1], [2]],
    )

    result = FTDCReader(path).get_metric(set(), start, start + timedelta(seconds=1))

    assert set(result) == {"start", "value", "other"}
    assert all(len(points) == 2 for points in result.values())


def test_get_metric_skips_unrequested_metric_columns(tmp_path: Path) -> None:
    """Selective decoding preserves deltas around an unrequested column."""

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    path = write_ftdc(
        tmp_path / "metrics.interim",
        {"start": start, "ignored": 100, "wanted": 10},
        [[1000, 1000], [0, 5], [2, 0]],
    )

    result = FTDCReader(path).get_metric({"wanted"})

    assert [point.value for point in result["wanted"]] == [10, 12, 12]


def test_missing_metric_raises(tmp_path: Path) -> None:
    """A requested metric absent from every chunk raises a specific error."""

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    path = write_ftdc(tmp_path / "metrics.interim", {"start": start, "value": 1}, [[], []])

    with pytest.raises(MetricNotFoundError):
        FTDCReader(path).get_metric({"value", "other"}, start, start)


def test_rejects_empty_metric_name(tmp_path: Path) -> None:
    """An empty string is not a valid requested metric name."""

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="metric names"):
        FTDCReader(tmp_path).get_metric({""}, start, start)


def test_rejects_naive_timespan(tmp_path: Path) -> None:
    """Timespan bounds must include timezone information."""

    reader = FTDCReader(tmp_path)

    with pytest.raises(ValueError, match="timezone-aware"):
        reader.get_metric({"value"}, datetime(2026, 1, 1), datetime(2026, 1, 2))


def test_get_metric_samples_points(tmp_path: Path) -> None:
    """A sample rate uniformly skips points for every requested metric."""

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    path = write_ftdc(
        tmp_path / "metrics.interim",
        {"start": start, "value": 0, "other": 10},
        [[1000] * 9, [1] * 9, [2] * 9],
    )

    result = FTDCReader(path).get_metric(
        {"value", "other"},
        start,
        start + timedelta(seconds=9),
        sample_rate=0.1,
    )

    assert [(point.timestamp, point.value) for point in result["value"]] == [
        (start + timedelta(seconds=9), 9)
    ]
    assert [(point.timestamp, point.value) for point in result["other"]] == [
        (start + timedelta(seconds=9), 28)
    ]


@pytest.mark.parametrize("sample_rate", [0, -0.1, 1.1, float("nan"), float("inf")])
def test_rejects_invalid_sample_rate(tmp_path: Path, sample_rate: float) -> None:
    """A sample rate must be finite and in the interval (0, 1]."""

    reader = FTDCReader(tmp_path)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="sample_rate"):
        reader.get_metric({"value"}, start, start, sample_rate=sample_rate)
