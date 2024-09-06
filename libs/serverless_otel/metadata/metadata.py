import os
from dataclasses import dataclass
from typing import Any, Final, List

import boto3

from serverless_otel.common import str_as_bool, TimeRange, SegmentMetadata

USE_SQLITE_STORAGE: Final[bool] = str_as_bool(os.getenv('USE_SQLITE_STORAGE', 'True'))
USE_COLUMNFILE_STORAGE: Final[bool] = str_as_bool(os.getenv('USE_COLUMNFILE_STORAGE', 'False'))
STORAGE_BASE_PATH: Final[str] = os.getenv('SHARED_STORAGE_BASEDIR', '/mnt/otel-hot/segments')

@dataclass
class MetadataContext:
    profile: str
    table_name: str
    region: str


_dynamo_client = None
def _lazy_initialise_dynamo(ctx: MetadataContext) -> Any:
    global _dynamo_client

    if _dynamo_client is not None:
        return _dynamo_client

    if ctx.profile is None:
        _dynamo_client = boto3.resource('dynamodb', region_name=ctx.region)
    else:
        __AWS_SESSION__ = boto3.Session(profile_name=ctx.profile, region_name=ctx.region)
        _dynamo_client = __AWS_SESSION__.client('dynamodb')

    return _dynamo_client


def segment_hot(ctx: MetadataContext, meta: SegmentMetadata) -> None:
    pass

def segment_warm(ctx: MetadataContext, dataset_id: str, range: TimeRange) -> None:
    pass

def segment_cold(ctx: MetadataContext, dataset_id: str, range: TimeRange) -> None:
    pass

def get_segment(ctx: MetadataContext, dataset_id: str, range: TimeRange) -> SegmentMetadata:
    pass

def find_segments(ctx: MetadataContext, dataset_id: str, range: TimeRange, columns: List[str] = None) -> List[SegmentMetadata]:
    pass