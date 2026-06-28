"""Public value objects."""

from dataclasses import dataclass
from datetime import datetime
from typing import TypeAlias

MetricValue: TypeAlias = int | float | bool


@dataclass(frozen=True, slots=True)
class DataPoint:
    """One metric observation."""

    timestamp: datetime
    value: MetricValue
