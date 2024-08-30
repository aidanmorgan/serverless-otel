import csv
import io
import json
import math
from sqlite3 import Connection
from typing import List, Dict
from uuid import uuid4

from filesystem_mutex import *
from s3_mutex import *

import sqlite3

tracer = trace.get_tracer(__name__)

def _create_csv_string(correlation_id: str, timestamp: str, value: str) -> str:
    # we cant control what a bunch of this stuff is, so we need to just use a proper CSV formatter to format the output
    # if DuckDB ends up supporting fixed width binary files then we could swap to that
    stream = io.StringIO()
    cw = csv.writer(stream)
    cw.writerow([timestamp, correlation_id, value])

    return stream.getvalue()


# A callable that will return the current UTC time in nanoseconds since epoch, set here to allow
# time to be injected externally if desired
__UTC_NOW_NANOS__: Callable[[], int] = lambda: time.time_ns()

# a callable that will take the values and format them appropriately for appending to a segment file
__FORMAT_SEGMENT_LINE__: Callable[[str, str, str], str] = _create_csv_string

# a primitive mechanism to tell lambda runtime hosts apart from each other to allow
# the NFS-locking mechanism to work, this SHOULD survive multiple invocations of the lambda
# if the lambda is already warm
__INSTANCE_ID__: Final[str] = uuid4().hex

# this needs to be the mount point for the NFS share that is common to all of the lambdas
__STORAGE_BASE_PATH__: Final[str] = os.getenv('SHARED_STORAGE_BASEDIR', default='/mnt/otel-hot/segments')

# in an attempt to keep the file sizes at a manageable level, we try to partition the files into
# roughly this number of minutes of data, which allows the query engine side to work out which
# files to parse
__SEGMENT_BUCKET_SIZE_MINUTES__: Final[int] = int(os.getenv('SEGMENT_BUCKET_SIZE_MINUTES', default='15'))

# which keys are considered required and therefore shouldn't be written as their own column files,
# these values will be embedded into each column file to allow the files to be combined
__REQUIRED_KEYS__: Final[List[str]] = ['timestamp-ms', 'timestamp-ns', 'correlation-id', 'dataset-id']

# the data types that are supported for column files, any field names that do not end in these
# (other than __IGNORE_KEYS__) are ignored when writing to the file system
__ALLOWED_DATA_TYPE_SUFFIXES__: Final[List[str]] = ['.int64', '.varchar', '.float64', '.bool', '.datetime']

# How we should be controlling access to the files - either NFS, S3 or None (default)
# NFI what will happen if you turn them both on other than a spectacular deadlock :-|
__USE_FILESYSTEM_MUTEX__: Final[bool] = True
__USE_S3_MUTEX__: Final[bool] = False

__USE_COLUMNFILES_STORAGE__: Final[bool] = False
__USE_SQLITE_STORAGE__: Final[bool] = True


def lambda_handler(event, context):
    telemetry_dict: Dict[str, str] = _body_to_dict(event)
    dataset_id: str = telemetry_dict['dataset-id']

    # this is a primitive mechanism to break the files down into smaller chunks by putting
    # them into separate directories that contain files of __SEGMENT_BUCKET_SIZE_MINUTES__
    # minutes worth of data
    segment_id: str = _make_segment_identifier(int(telemetry_dict['timestamp-ns']))

    if __USE_SQLITE_STORAGE__:
        return _lambda_handler_sqlite(event, context, dataset_id, segment_id, telemetry_dict)

    if __USE_COLUMNFILES_STORAGE__:
        return _lambda_handler_files(event, context, dataset_id, segment_id, telemetry_dict)

def _lambda_handler_sqlite(event, context, dataset_id: str, segment_id: str, telemetry_dict: Dict[str,Any]):
    nfs_initialise_segment_locks(__STORAGE_BASE_PATH__, dataset_id, __INSTANCE_ID__, segment_id)

    with tracer.start_as_current_span('_lambda_handler_sqlite') as span:
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
            if __USE_FILESYSTEM_MUTEX__:
                lock = nfs_lock_segment(__STORAGE_BASE_PATH__, dataset_id, segment_id, __INSTANCE_ID__, __UTC_NOW_NANOS__)

            if __USE_S3_MUTEX__:
                lock = s3_lock_segment(dataset_id, segment_id, __INSTANCE_ID__, __UTC_NOW_NANOS__)

            data_file: str = os.path.join(__STORAGE_BASE_PATH__, dataset_id, segment_id, f'{segment_id}.sqlite')
            span.set_attribute('database_path', data_file)

            database_exists: bool = os.path.exists(data_file)

            con = sqlite3.connect(data_file)
            con.execute('PRAGMA journal_mode = WAL')
            con.execute('PRAGMA synchronous = NORMAL')
            con.execute('PRAGMA temp_store = memory')

            if not database_exists:
                span.add_event(f'created database')
                con.execute('CREATE TABLE segment_data (correlation_id TEXT PRIMARY KEY, timestamp INTEGER, payload TEXT)')

            con.execute('INSERT INTO segment_data(timestamp, correlation_id, payload) VALUES (?, ?, ?)', (timestamp, correlation_id, json.dumps(telemetry_dict)))
            con.commit()
        except BodyError as err:
            print(f'Invalid body content for Segment {segment_id}. {err}')
            return 400
        except SegmentLockError as err:
            print(f'Error locking Segment {segment_id}. {err}')
            return 500
        finally:
            try:
                if __USE_FILESYSTEM_MUTEX__:
                    nfs_unlock_segment(__STORAGE_BASE_PATH__, dataset_id, segment_id, __INSTANCE_ID__, lock)

                if __USE_S3_MUTEX__:
                    s3_unlock_segment(dataset_id, segment_id, __INSTANCE_ID__, lock)
            except SegmentLockError as err:
                print(f'Error unlocking Segment {segment_id}. {err}')

            con.close()

# implementation of the lambda handler that uses files for storing
def _lambda_handler_files(event, context, dataset_id: str, segment_id: str, telemetry_dict: Dict[str,Any]):
    timestamp: str = telemetry_dict['timestamp-ns']
    correlation_id: str = telemetry_dict['correlation-id']

    # the initialise_segment_locks ensures that the directories that are needed for storing the files are in
    # place, as well as their corresponding lock directories
    nfs_initialise_segment_locks(__STORAGE_BASE_PATH__, dataset_id, __INSTANCE_ID__, segment_id)

    with tracer.start_as_current_span('_lambda_handler_files') as span:
        span.set_attribute(f'dataset_id', dataset_id)
        span.set_attribute(f'segment_id', segment_id)
        span.set_attribute(f'correlation_id', correlation_id)
        span.set_attribute(f'timestamp', timestamp)
        span.set_attribute(f'keys', ','.join(telemetry_dict.keys()))

        try:
            # these are very coarse-grained locks, we're locking the whole segment (15 minutes of data) could probably be
            # refined to actually lock the specific file we are attempting to update, and probably even run in parallel
            # HOWEVER - this then creates a weird rollback situation if we have been able to write to some files, but not all
            # then we have thrown away data, so leaving this as the coarse-grained lock for the time being...
            if __USE_FILESYSTEM_MUTEX__:
                nfs_lock_segment(__STORAGE_BASE_PATH__, dataset_id, segment_id, __INSTANCE_ID__, __UTC_NOW_NANOS__)

            if __USE_S3_MUTEX__:
                s3_lock_segment(dataset_id, segment_id, __INSTANCE_ID__, __UTC_NOW_NANOS__)

            for key in telemetry_dict.keys():
                if key in __REQUIRED_KEYS__ or not any(
                        suffix for suffix in __ALLOWED_DATA_TYPE_SUFFIXES__ if key.endswith(suffix)):
                    continue

                _append_record(dataset_id, segment_id, timestamp, correlation_id, key, telemetry_dict[key])

            return 200
        except BodyError as err:
            print(f'Invalid body payload. {err}')
            return 503
        except SegmentLockError as err:
            print(f'Could not lock segment {segment_id} for instance {__INSTANCE_ID__}, {err}')
            return 503
        finally:
            try:
                if __USE_FILESYSTEM_MUTEX__:
                    nfs_unlock_segment(__STORAGE_BASE_PATH__, dataset_id, segment_id, __INSTANCE_ID__)

                if __USE_S3_MUTEX__:
                    s3_unlock_segment(dataset_id, segment_id, __INSTANCE_ID__)
            except SegmentLockError as err:
                print(f'Could not unlock segment {segment_id} for instance {__INSTANCE_ID__}, {err}')


def _make_segment_identifier(current_nanos: int):
    # truncate the time to the correct multiple of __SEGMENT_BUCKET_SIZE_MINUTES__ as the identifier
    whole: int = math.floor(current_nanos / (__SEGMENT_BUCKET_SIZE_MINUTES__ * NS_PER_MIN))
    return f'segment-{int(whole * (__SEGMENT_BUCKET_SIZE_MINUTES__ * NS_PER_MIN))}'


def _get_file_path_for_column(basedir: str, dataset_id: str, segment: str, key: str) -> str:
    data_file: str = os.path.join(basedir, dataset_id, segment, f'{key}')

    if not os.path.exists(data_file):
        with open(data_file, mode='a'):
            pass

    return data_file


def _body_to_dict(event) -> Dict[str, str]:
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
    path: str = _get_file_path_for_column(__STORAGE_BASE_PATH__, dataset_id, segment_id, key)
    line: str = __FORMAT_SEGMENT_LINE__(timestamp, correlation_id, value)

    with open(path, 'a') as column_file:
        # append the value to the column file
        column_file.write(f'{line}\n')
        column_file.flush()


class BodyError(Exception):
    pass
