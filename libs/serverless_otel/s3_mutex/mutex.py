import datetime
import os
import time
from collections import namedtuple
from typing import Callable, Final, Dict, Any, Optional

import boto3
from botocore.exceptions import ClientError
from opentelemetry import trace
from opentelemetry.trace import StatusCode

from constants import NS_PER_S
from serverless_otel.common.errors import SegmentLockError, SegmentUnlockError

PROFILE_NAME: Final[Optional[str]] = os.getenv('PROFILE_NAME', None)
BUCKET_NAME: Final[str] = os.getenv('SEGMENT_LOCK_BUCKET', 'dev-serverless-otel-segments')
LOCK_TTL_SECONDS: Final[int] = int(os.getenv('SEGMENT_LOCK_TTL', '300'))

S3LockIdentifier = namedtuple('S3LockIdentifier', ['ETag', 'Timestamp'])
tracer = trace.get_tracer(__name__)

_dynamo_client = None


def lazy_initialise_s3() -> Any:
    global _dynamo_client

    if _s3_client is not None:
        return _s3_client

    with tracer.start_as_current_span('lazy_initialise_s3') as span:
        if PROFILE_NAME is None:
            _s3_client = boto3.resource('s3')
        else:
            __AWS_SESSION__ = boto3.Session(profile_name=PROFILE_NAME)
            _s3_client = __AWS_SESSION__.client('s3')

        return _s3_client


# this is an attempt to make the instantiation of the boto library to s3 a 'lazy evaluation' so we can pick
# up the required class fields from the environment at the first attempt to use any s3 mutex
__S3_CLIENT_FACTORY__: Callable[[], Any] = lazy_initialise_s3


def s3_initialise_segment_locks(dataset_id: str, instance: str, segment: str):
    pass


def s3_lock_segment(dataset_id: str, segment: str, instance: str, utc_nanos: Callable[[], int],
                    timeout: int = 300, delay: int = 5) -> S3LockIdentifier:
    start: int = utc_nanos()
    key: str = f'{dataset_id}/{segment}'

    with tracer.start_as_current_span('s3_lock_segment') as span:
        span.set_attribute('bucket', BUCKET_NAME)
        span.set_attribute('key', key)

        while (utc_nanos() - start) < (timeout * NS_PER_S):
            try:
                # the body of the file seems stupid, however the ETag is based on these values, so by making sure we have some concept of time and the instance
                # that is attempting to write to the object then we maybe/should/potentially can make sure that we're referring to the
                # same lock file when we go to unlock?
                lock_id: Dict[str, Any] = __S3_CLIENT_FACTORY__().put_object(Bucket=BUCKET_NAME, Key=key,
                                                                             Body=str.encode(
                                                                                 f'{instance}:{time.time_ns()}'),
                                                                             IfNoneMatch='*',
                                                                             Tagging=f'instance_id={instance}',
                                                                             Expires=datetime.datetime.now() + datetime.timedelta(
                                                                                 seconds=LOCK_TTL_SECONDS))

                span.set_attribute('ETag', lock_id['ETag'])
                span.set_status(StatusCode.OK)
                return S3LockIdentifier(ETag=lock_id['ETag'].strip('"'), Timestamp=utc_nanos())
            except ClientError as err:
                # we are using the conditional write to determine if the file already exists, if it does then
                if err.response['ResponseMetadata']['HTTPStatusCode'] not in [409, 412]:
                    span.set_status(StatusCode.ERROR, f'Unexpected HTTP status code. {err}')
                    span.record_exception(err)
                    raise SegmentLockError(f'Cannot acquire lock, communication error. {err}')
            except Exception as err:
                span.set_status(StatusCode.ERROR, f'Unexpected error. {err}')
                span.record_exception(err)
                raise SegmentLockError(f'Cannot acquire lock, unexpected error. {err}')

        span.set_status(StatusCode.ERROR, f'Acquiring lock timed out.')
        raise SegmentLockError(f'Cannot acquire lock, timeout after {timeout} seconds. {segment}')


def s3_unlock_segment(dataset_id: str, segment: str, instance: str, lock: S3LockIdentifier) -> None:
    # this would be easier if the ETag calculation doesn't magically change on the AWS side, because we could just
    # compute the ETag value and not need to pass around the lock object, but /shrug
    key: str = f'{dataset_id}/{segment}'

    with tracer.start_as_current_span('s3_unlock_segment') as span:
        span.set_attribute('bucket', BUCKET_NAME)
        span.set_attribute('key', key)
        span.set_attribute('ETag', lock.ETag)

        if lock is None:
            span.set_status(StatusCode.ERROR, f'No lock held')
            raise SegmentUnlockError('Cannot release lock, no lock obtained.')
        try:
            head: Dict[str, Any] = __S3_CLIENT_FACTORY__().head_object(Bucket=BUCKET_NAME, Key=key, IfMatch=lock.ETag)

            if head is None:
                span.set_status(StatusCode.ERROR, f'Not owner of lock')
                raise SegmentUnlockError('Cannot release lock, not owner')
        except ClientError as err:
            span.set_status(StatusCode.ERROR, f'Unexpected error. {err}')
            span.record_exception(err)
            raise SegmentUnlockError('Cannot release lock, not owner')

        try:
            __S3_CLIENT_FACTORY__().delete_object(Bucket=BUCKET_NAME, Key=f'{dataset_id}/{segment}')
            span.set_status(StatusCode.OK)
            return
        except ClientError as err:
            span.set_status(StatusCode.ERROR, f'Unexpected error. {err}')
            span.record_exception(err)
            raise SegmentUnlockError(f'Cannot release lock, communication error. {err}')
        except Exception as x:
            span.set_status(StatusCode.ERROR, f'Unexpected error. {x}')
            span.record_exception(x)
            raise SegmentUnlockError(f'Cannot release lock, unknown error. {x}')
