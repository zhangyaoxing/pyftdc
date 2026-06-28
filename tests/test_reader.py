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

    points = FTDCReader(tmp_path).get_metric(
        "serverStatus.connections.current",
        start + timedelta(seconds=1),
        start + timedelta(seconds=2),
    )

    assert [point.timestamp for point in points] == [
        start + timedelta(seconds=1),
        start + timedelta(seconds=2),
    ]
    assert [point.value for point in points] == [12, 12]


def test_missing_metric_raises(tmp_path: Path) -> None:
    """A metric absent from every chunk raises a specific error."""

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    path = write_ftdc(tmp_path / "metrics.interim", {"start": start, "value": 1}, [[], []])

    with pytest.raises(MetricNotFoundError):
        FTDCReader(path).get_metric("other", start, start)


def test_rejects_naive_timespan(tmp_path: Path) -> None:
    """Timespan bounds must include timezone information."""

    reader = FTDCReader(tmp_path)

    with pytest.raises(ValueError, match="timezone-aware"):
        reader.get_metric("value", datetime(2026, 1, 1), datetime(2026, 1, 2))
