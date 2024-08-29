import os
import time
from collections import namedtuple
from typing import Callable, Final, Dict, Any, Optional
import boto3
import datetime

from constants import NS_PER_S
from botocore.exceptions import ClientError
from filesystem_mutex import SegmentLockError, SegmentUnlockError

__PROFILE_NAME__: Final[Optional[str]] = os.getenv('PROFILE_NAME', None)
__BUCKET_NAME__: Final[str] = os.getenv('SEGMENT_LOCK_BUCKET', 'dev-serverless-otel-segments')
__LOCK_TTL_SECONDS__: Final[int] = int(os.getenv('SEGMENT_LOCK_TTL', '300'))

S3LockIdentifier = namedtuple('S3LockIdentifier', ['ETag'])

_s3_client = None
def initialise_s3() -> Any:
    global _s3_client

    if _s3_client is not None:
        return _s3_client

    if __PROFILE_NAME__ is None:
        _s3_client = boto3.resource('s3')
    else:
        __AWS_SESSION__ = boto3.Session(profile_name=__PROFILE_NAME__)
        _s3_client = __AWS_SESSION__.client('s3')

    return _s3_client

# this is an attempt to make the instantiation of the boto library to s3 a 'lazy evaluation' so we can pick
# up the required class fields from the environment at the first attempt to use any s3 mutex
__S3_CLIENT__: Callable[[], Any] = initialise_s3


def s3_initialise_segment_locks(dataset_id: str, instance: str, segment: str):
    pass


def s3_lock_segment(dataset_id: str, segment: str, instance: str, utc_nanos: Callable[[], int],
                 timeout: int = 300, delay: int = 5) -> S3LockIdentifier:
    start: int = utc_nanos()

    while (utc_nanos() - start) < (timeout * NS_PER_S):
        try:
            lock_id: Dict[str,Any] = __S3_CLIENT__().put_object(Bucket=__BUCKET_NAME__, Key=f'{dataset_id}/{segment}', Body=str.encode(f'{instance}:{time.time_ns()}'), IfNoneMatch='*', Tagging=f'instance_id={instance}', Expires=datetime.datetime.now() + datetime.timedelta(seconds=__LOCK_TTL_SECONDS__))
            return S3LockIdentifier(lock_id['ETag'].strip('"'))
        except ClientError as err:
            # we are using the conditional write to determine if the file already exists, if it does then
            if err.response['ResponseMetadata']['HTTPStatusCode'] not in [409, 412]:
                raise SegmentLockError(f'Cannot acquire lock, communication error. {err}')
        except Exception as err:
            raise SegmentLockError(f'Cannot acquire lock, unexpected error. {err}')

    raise SegmentLockError(f'Cannot acquire lock, timeout after {timeout} seconds. {segment}')

def s3_unlock_segment(dataset_id: str, segment: str, instance: str, lock: S3LockIdentifier) -> None:
    if lock is None:
        raise SegmentUnlockError('Cannot release lock, no lock obtained.')
    try:
        head: Dict[str,Any] = __S3_CLIENT__().head_object(Bucket=__BUCKET_NAME__, Key=f'{dataset_id}/{segment}', IfMatch=lock.ETag)

        if head is None:
            raise SegmentUnlockError('Cannot release lock, not owner')

    except ClientError as err:
        raise SegmentUnlockError('Cannot release lock, not owner')

    try:
        __S3_CLIENT__().delete_object(Bucket=__BUCKET_NAME__, Key=f'{dataset_id}/{segment}')
        return
    except ClientError as err:
        raise SegmentUnlockError(f'Cannot release lock, communication error. {err}')
    except Exception as x:
        raise SegmentUnlockError(f'Cannot release lock, unknown error. {x}')

