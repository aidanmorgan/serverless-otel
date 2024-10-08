import csv
import io
import json
import math
import sqlite3
import time
import os
from sqlite3 import Connection
from typing import List, Dict, Optional, Any, Callable
from uuid import uuid4

from aws_lambda_typing.events import APIGatewayProxyEventV2
import aws_lambda_typing.context as context_

from opentelemetry.trace import Tracer, get_tracer

from serverless_otel.common import make_segment_identifier
from serverless_otel.common.constants import *
from serverless_otel.common.env_utils import str_as_bool
from serverless_otel.common.errors import *

from serverless_otel.filesystem_storage import determine_filesystem_path
from serverless_otel.sqlite_storage import determine_sqlite_file_path

from serverless_otel.s3_mutex import s3_lock_segment, s3_unlock_segment, s3_initialise_segment_locks
from serverless_otel.nfs_mutex import nfs_lock_segment, nfs_unlock_segment, nfs_initialise_segment_locks


def _create_csv_string(correlation_id: str, timestamp: str, value: str) -> str:
    # we cant control what a bunch of this stuff is, so we need to just use a proper CSV formatter to format the output
    # if DuckDB ends up supporting fixed width binary files then we could swap to that
    stream = io.StringIO()
    cw = csv.writer(stream)
    cw.writerow([timestamp, correlation_id, value])

    return stream.getvalue()


# A callable that will return the current UTC time in nanoseconds since epoch, set here to allow
# time to be injected externally if desired
UTC_NOW_NANOS: Callable[[], int] = lambda: time.time_ns()

# a callable that will take the values and format them appropriately for appending to a segment file
FORMAT_SEGMENT_LINE: Callable[[str, str, str], str] = _create_csv_string

# a primitive mechanism to tell lambda runtime hosts apart from each other to allow
# the NFS-locking mechanism to work, this SHOULD survive multiple invocations of the lambda
# if the lambda is already warm
INSTANCE_ID: Final[str] = uuid4().hex

# this needs to be the mount point for the NFS share that is common to all of the lambdas

# which keys are considered required and therefore shouldn't be written as their own column files,
# these values will be embedded into each column file to allow the files to be combined
REQUIRED_KEYS: Final[List[str]] = ['timestamp-ms', 'timestamp-ns', 'correlation-id', 'dataset-id']

# the data types that are supported for column files, any field names that do not end in these
# (other than __IGNORE_KEYS__) are ignored when writing to the file system
ALLOWED_DATA_TYPE_SUFFIXES: Final[List[str]] = ['.int64', '.varchar', '.float64', '.bool', '.datetime']

# How we should be controlling access to the files - either NFS, S3 or None (default)
# NFI what will happen if you turn them both on other than a spectacular deadlock :-|
USE_FILESYSTEM_MUTEX: Final[bool] = str_as_bool(os.getenv('USE_FILESYSTEM_MUTEX', 'True'))
USE_S3_MUTEX: Final[bool] = str_as_bool(os.getenv('USE_S3_MUTEX', 'False'))

USE_SQLITE_STORAGE: Final[bool] = str_as_bool(os.getenv('USE_SQLITE_STORAGE', 'True'))
USE_COLUMNFILE_STORAGE: Final[bool] = str_as_bool(os.getenv('USE_COLUMNFILE_STORAGE', 'False'))

STORAGE_BASE_PATH: Final[str] = os.getenv('SHARED_STORAGE_BASEDIR', '/mnt/otel-hot/segments')
SEGMENT_BUCKET_SIZE_MINUTES: Final[int] = int(os.getenv('SEGMENT_BUCKET_SIZE_MINUTES', '15'))

tracer:Tracer = get_tracer(__name__)


def lambda_handler(event: APIGatewayProxyEventV2, context: context_.Context):
    telemetry_dict: Dict[str, str] = _body_to_dict(event)
    dataset_id: str = telemetry_dict['dataset-id']

    # this is a primitive mechanism to break the files down into smaller chunks by putting
    # them into separate directories that contain files of __SEGMENT_BUCKET_SIZE_MINUTES__
    # minutes worth of data
    segment_id: str = make_segment_identifier(int(telemetry_dict['timestamp-ns']))

    if USE_SQLITE_STORAGE:
        return _lambda_handler_sqlite(event, context, dataset_id, segment_id, telemetry_dict)

    if USE_COLUMNFILE_STORAGE:
        return _lambda_handler_files(event, context, dataset_id, segment_id, telemetry_dict)


def _lambda_handler_sqlite(event, context, dataset_id: str, segment_id: str, telemetry_dict: Dict[str, Any]):
    nfs_initialise_segment_locks(STORAGE_BASE_PATH, dataset_id, INSTANCE_ID, segment_id)

    with tracer.start_as_current_span('lambda_handler_sqlite') as span:
        con: Optional[Connection] = None
        lock: Optional[Any] = None

        timestamp: str = telemetry_dict['timestamp-ns']
        correlation_id: str = telemetry_dict['correlation-id']

        span.set_attribute('dataset_id', dataset_id)
        span.set_attribute('segment_id', segment_id)
        span.set_attribute('correlation_id', correlation_id)
        span.set_attribute('timestamp', timestamp)
        span.set_attribute('keys', ','.join(telemetry_dict.keys()))

        try:
            if USE_FILESYSTEM_MUTEX:
                lock = nfs_lock_segment(STORAGE_BASE_PATH, dataset_id, segment_id, INSTANCE_ID, UTC_NOW_NANOS)

            if USE_S3_MUTEX:
                lock = s3_lock_segment(dataset_id, segment_id, INSTANCE_ID, UTC_NOW_NANOS)

            data_file:str = determine_sqlite_file_path(STORAGE_BASE_PATH, dataset_id, segment_id)
            span.set_attribute('database_path', data_file)

            database_exists: bool = os.path.exists(data_file)

            con = sqlite3.connect(data_file)
            con.execute('PRAGMA journal_mode = WAL')
            con.execute('PRAGMA synchronous = NORMAL')
            con.execute('PRAGMA temp_store = memory')

            if not database_exists:
                span.add_event(f'created database')
                con.execute(
                    'CREATE TABLE segment_data (correlation_id TEXT PRIMARY KEY, timestamp INTEGER, payload TEXT)')

            con.execute('INSERT INTO segment_data(timestamp, correlation_id, payload) VALUES (?, ?, ?)',(timestamp, correlation_id, json.dumps(telemetry_dict)))
            con.commit()

            print(f'Wrote value to {data_file}.')

            return 201
        except BodyError as err:
            print(f'Invalid body content for Segment {segment_id}. {err}')
            return 400
        except SegmentLockError as err:
            print(f'Error locking Segment {segment_id}. {err}')
            return 500
        finally:
            try:
                if USE_FILESYSTEM_MUTEX:
                    nfs_unlock_segment(STORAGE_BASE_PATH, dataset_id, segment_id, INSTANCE_ID, lock)

                if USE_S3_MUTEX:
                    s3_unlock_segment(dataset_id, segment_id, INSTANCE_ID, lock)
            except SegmentLockError as err:
                print(f'Error unlocking Segment {segment_id}. {err}')

            con.close()


# implementation of the lambda handler that uses files for storing
def _lambda_handler_files(event, context, dataset_id: str, segment_id: str, telemetry_dict: Dict[str, Any]):
    timestamp: str = telemetry_dict['timestamp-ns']
    correlation_id: str = telemetry_dict['correlation-id']

    # the initialise_segment_locks ensures that the directories that are needed for storing the files are in
    # place, as well as their corresponding lock directories
    nfs_initialise_segment_locks(STORAGE_BASE_PATH, dataset_id, INSTANCE_ID, segment_id)

    with tracer.start_as_current_span('lambda_handler_files') as span:
        span.set_attribute('dataset_id', dataset_id)
        span.set_attribute('segment_id', segment_id)
        span.set_attribute('correlation_id', correlation_id)
        span.set_attribute('timestamp', timestamp)
        span.set_attribute('keys', ','.join(telemetry_dict.keys()))

        try:
            # these are very coarse-grained locks, we're locking the whole segment (15 minutes of data) could probably be
            # refined to actually lock the specific file we are attempting to update, and probably even run in parallel
            # HOWEVER - this then creates a weird rollback situation if we have been able to write to some files, but not all
            # then we have thrown away data, so leaving this as the coarse-grained lock for the time being...
            if USE_FILESYSTEM_MUTEX:
                nfs_lock_segment(STORAGE_BASE_PATH, dataset_id, segment_id, INSTANCE_ID, UTC_NOW_NANOS)

            if USE_S3_MUTEX:
                s3_lock_segment(dataset_id, segment_id, INSTANCE_ID, UTC_NOW_NANOS)

            for key in telemetry_dict.keys():
                if key in REQUIRED_KEYS or not any(
                        suffix for suffix in ALLOWED_DATA_TYPE_SUFFIXES if key.endswith(suffix)):
                    continue

                _append_record(dataset_id, segment_id, timestamp, correlation_id, key, telemetry_dict[key])

            return 201
        except BodyError as err:
            print(f'Invalid body payload. {err}')
            return 503
        except SegmentLockError as err:
            print(f'Could not lock segment {segment_id} for instance {INSTANCE_ID}, {err}')
            return 503
        finally:
            try:
                if USE_FILESYSTEM_MUTEX:
                    nfs_unlock_segment(STORAGE_BASE_PATH, dataset_id, segment_id, INSTANCE_ID)

                if USE_S3_MUTEX:
                    s3_unlock_segment(dataset_id, segment_id, INSTANCE_ID)
            except SegmentLockError as err:
                print(f'Could not unlock segment {segment_id} for instance {INSTANCE_ID}, {err}')



def _body_to_dict(event: APIGatewayProxyEventV2) -> Dict[str, str]:
    body: str = event['body']

    key_value: Dict[str, str] = dict()

    # split by new lines, then break each line into key=value pairs
    for lines in body.split('\n'):
        split: List[str] = lines.split('=')

        if len(split) == 2:
            key_value[split[0].lower()] = split[1]

    # perform validation checks for the dictionary to make sure that the minimum
    # required fields are present in the dictionary to allow processing

    if 'dataset-id' not in key_value or len(key_value['dataset-id']) == 0:
        raise BodyError('No dataset-id specified')

    if 'timestamp-ns' not in key_value and 'timestamp-ms' not in key_value:
        raise BodyError('No timestamp specified')

    ms_value: int = 0
    ns_value: int = 0

    if 'timestamp-ns' in key_value:
        ms_value = math.floor(int(key_value['timestamp-ns']) / NS_PER_MS)
        ns_value = int(key_value['timestamp-ns'])

    if 'timestamp-ms' in key_value:
        ms_value = int(key_value['timestamp-ms'])
        ns_value = math.floor(int(key_value['timestamp-ms']) * NS_PER_MS)

    key_value['timestamp-ns'] = str(ns_value)
    key_value['timestamp-ms'] = str(ms_value)

    if 'correlation-id' not in key_value or len(key_value['correlation-id']) == 0:
        raise BodyError('No correlation-id specified')

    return key_value


def _append_record(dataset_id: str, segment_id: str, timestamp: str, correlation_id: str, key: str, value: str):
    path: str = determine_filesystem_path(STORAGE_BASE_PATH, dataset_id, segment_id, key)
    line: str = FORMAT_SEGMENT_LINE(timestamp, correlation_id, value)

    with open(path, 'a') as column_file:
        # append the value to the column file
        column_file.write(f'{line}\n')
        column_file.flush()
