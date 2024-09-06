from enum import Enum, StrEnum


class StorageType(Enum):
    """
    Top level guidance of what underlying storage engine is currently being used, either
    a 'honeycomb-like' file system storage, or a SQLite storage system.
    """
    FILE_BASED = 1
    SQLITE = 2

class SegmentState(Enum):
    """
    A segment is a specific 'chunk' of time of a dataset, that is it represents
    a finite window of time for a dataset with it's associated columns.

    A HOT segment is one that is actively been updated, or the time window that it represents
    is still open.

    A WARM segment is one that is considered closed, but is retained in the hot store to allow
    for late-arriving data, that is, the window of time it represents has closed.

    A COLD segment is one that is closed and has been moved to secondary storage, it can no
    longer be updated (even by late arriving data).

    TODO: Consider whether COLD data can still be updated, we would just need to re-export the
        : Parquet file (or at least get the parquet file, add the new data and save it again)
    """
    HOT = 1
    WARM = 2
    COLD = 3

class ColumnType(StrEnum):
    """
    The allowable data types that we will store for each column - intentionally limited for experimental purposes
    """
    INTEGER = 'int64'
    UNSIGNED_INTEGER = 'uint64'
    FLOAT = 'float64'
    BOOLEAN = 'bool'
    STRING = 'varchar'
    TIMESTAMP = 'datetime'
