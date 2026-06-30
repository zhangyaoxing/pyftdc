# pymongoftdc

[![CI](https://github.com/zhangyaoxing/pyftdc/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/zhangyaoxing/pyftdc/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pymongoftdc.svg)](https://pypi.org/project/pymongoftdc/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

`pymongoftdc` reads numeric time-series metrics directly from MongoDB Full-Time
Diagnostic Data Capture (FTDC) archive files.

## Install

```bash
python -m pip install -e .
```

For development:

```bash
python -m pip install -e '.[test]'
pytest
```

## Use

```python
from datetime import datetime, timezone
from pyftdc import FTDCReader

reader = FTDCReader("/var/lib/mongo/diagnostic.data")
metrics = reader.get_metric(
    {"serverStatus.connections.current"},
    start=datetime(2026, 1, 1, tzinfo=timezone.utc),
    end=datetime(2026, 1, 1, 1, tzinfo=timezone.utc),
    sample_rate=0.1,
    sort_by_timestamp=False,
    workers=None,
)
points = metrics["serverStatus.connections.current"]
```

The source may be one `metrics.*` file or a directory that contains multiple metrics files.
- Timespan endpoints are inclusive and must be timezone-aware. Omit `start` or 
  `end` to use the earliest or latest timestamp in the source. 
- The result maps each requested name to points in source traversal order. 
  Pass `sort_by_timestamp=True` to force order each point list by UTC timestamp (Usually unnecessary).
- Pass an empty set to read every metric. 
- `sample_rate` must be greater than 0 and at most 1. For example, `0.1` returns approximately 
  10% of points. Its default is `1.0`.
- Metric chunks are decoded in separate processes. By default, `workers` is the
  detected CPU count minus one, with a minimum of one. Set `workers=1` to disable
  multiprocessing or choose a smaller value to limit memory use.
- `query()` is an alias for `get_metric()`.
- Use `reader.list_metrics()` to discover dotted metric paths from the first metric
  chunk. Pass `all_chunks=True` to scan the full source for schema changes.
- A missing requested metric raises `MetricNotFoundError`; an invalid archive raises `FTDCDecodeError`.

## Project layout

```text
src/pyftdc/
  _codec.py       BSON framing and FTDC decompression
  reader.py       public query API
  models.py       returned value objects
  exceptions.py   library-specific errors
tests/             pytest tests and fixture builders
```

The reader supports BSON-framed type-1 metric chunks using MongoDB's
delta/RLE/varint/zlib encoding. Metadata documents are safely skipped.
