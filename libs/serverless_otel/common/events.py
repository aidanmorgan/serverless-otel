from dataclasses import dataclass
from uuid import uuid4

from serverless_otel.common import TimeRange, StorageType


class Event:
    event_id: str
    event_name: str

@dataclass
class SegmentCreated(Event):
    range: TimeRange
    storage_type: StorageType

    def __init__(self):
        self.event_id = uuid4().hex
        self.event_name = 'SegmentCreated'

@dataclass
class SegmentUpdated(Event):
    def __init__(self):
        self.event_id = uuid4().hex
        self.event_name = 'SegmentUpdated'

@dataclass
class SegmentArchived(Event):
    def __init__(self):
        self.event_id = uuid4().hex
        self.event_name = 'SegmentArchived'

@dataclass
class DatasetAdded(Event):
    def __init__(self):
        self.event_id = uuid4().hex
        self.event_name = 'DatasetAdded'

