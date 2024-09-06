from collections import namedtuple
from dataclasses import dataclass
from typing import Set, Dict

from .enums import ColumnType, StorageType, SegmentState

TimeRange = namedtuple('TimeRange', ['start_ts', 'end_ts'])

@dataclass
class ColumnMetadata:
    column_name: str
    column_type: ColumnType

@dataclass
class SegmentMetadata:
    dataset_id: str
    type: StorageType
    state: SegmentState
    range: TimeRange
    fields: Set[ColumnMetadata]

@dataclass
class SqliteSegment(SegmentMetadata):
    file_path: str

@dataclass
class FilesystemSegement(SegmentMetadata):
    base_path: str
    files: Dict[ColumnMetadata, str]

@dataclass
class ParquetSegment(SegmentMetadata):
    file_path: str

