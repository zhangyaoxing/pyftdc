"""Exceptions raised by pyftdc."""


class FTDCError(Exception):
    """Base class for pyftdc errors."""


class FTDCDecodeError(FTDCError):
    """An FTDC file or compressed metric chunk is invalid."""


class MetricNotFoundError(FTDCError, KeyError):
    """The requested metric does not occur in the source."""
