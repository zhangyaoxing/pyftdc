# pyftdc

`pyftdc` reads numeric time-series metrics directly from MongoDB Full-Time
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
points = reader.get_metric(
    "serverStatus.connections.current",
    start=datetime(2026, 1, 1, tzinfo=timezone.utc),
    end=datetime(2026, 1, 1, 1, tzinfo=timezone.utc),
)
```

The source may be one `metrics.*` file or a `diagnostic.data` directory.
Timespan endpoints are inclusive and must be timezone-aware. Results are
ordered by UTC timestamp. `query()` is an alias for `get_metric()`.

Use `reader.list_metrics()` to discover dotted metric paths. A missing metric
raises `MetricNotFoundError`; an invalid archive raises `FTDCDecodeError`.

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
