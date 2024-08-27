import datetime
from typing import List, Dict
from uuid import uuid4

from nfs_locks import *

# a primitive mechanism to tell lambda runtime hosts apart from each other to allow
# the NFS-locking mechanism to work, this SHOULD survive multiple invocations of the lambda
# if the lambda is already warm
__INSTANCE_ID__: str = uuid4().hex

# this needs to be the mount point for the NFS share that is common to all of the lambdas
__SEGMENT_BASEDIR__: str = os.getenv('SHARED_STORAGE_BASEDIR', default='/mnt/otel-hot/segments')

# in an attempt to keep the file sizes at a manageable level, we try to partition the files into
# roughly this number of minutes of data, which allows the query engine side to work out which
# files to parse
__SEGMENT_BUCKET_SIZE_MINUTES__: int = int(os.getenv('SEGMENT_BUCKET_SIZE_MINUTES', default='15'))

# which keys are considered required and therefore shouldn't be written as their own column files
__IGNORE_KEYS__: List[str] = ['timestamp', 'correlation-id', 'dataset-id']

# the data types that are supported for column files, any field names that do not end in these
# (other than __IGNORE_KEYS__) are ignored when writing to the file system
__ALLOWED_DATA_TYPE_SUFFIXES__: List[str] = ['.int64', '.varchar', '.float64', '.bool', '.datetime']


def lambda_handler(event, context):
    start_time: datetime.datetime = datetime.datetime.now()

    # this is a primitive mechanism to break the files down into smaller chunks by putting
    # them into separate directories that contain files of __SEGMENT_BUCKET_SIZE_MINUTES__
    # minutes worth of data
    segment_id: str = f'segment-{_get_segment_identifier(start_time)}'

    telemetry_dict: Dict[str, str] = _body_to_dict(event)
    dataset_id: str = telemetry_dict['dataset-id']

    initialise_segment_locks(__SEGMENT_BASEDIR__, dataset_id, __INSTANCE_ID__, segment_id)

    try:
        lock_segment(__SEGMENT_BASEDIR__, dataset_id, segment_id, __INSTANCE_ID__)

        for key in telemetry_dict.keys():
            if key in __IGNORE_KEYS__ or not any(suffix for suffix in __ALLOWED_DATA_TYPE_SUFFIXES__ if key.endswith(suffix)):
                continue

            _append_record(dataset_id, segment_id, telemetry_dict['timestamp'], telemetry_dict['correlation-id'], key, telemetry_dict[key])
    except BodyError as err:
        print(f'Invalid body payload. {err}')
    except SegmentLockError as err:
        print(f'Could not lock segment {segment_id} for instance {__INSTANCE_ID__}. {err}')
    finally:
        try:
            unlock_segment(__SEGMENT_BASEDIR__, dataset_id, segment_id, __INSTANCE_ID__)
        except SegmentLockError as err:
            print(f'Could not unlock segment {segment_id} for instance {__INSTANCE_ID__}')


def _get_segment_identifier(t : datetime.datetime):
    delta = datetime.timedelta(minutes=t.minute % __SEGMENT_BUCKET_SIZE_MINUTES__,
                               seconds=t.second,
                               microseconds=t.microsecond)
    t -= delta
    if delta > datetime.timedelta(0):
        t += datetime.timedelta(minutes=__SEGMENT_BUCKET_SIZE_MINUTES__)

    return t.timestamp() * 1000


def _get_file_path_for_column(basedir:str, dataset_id: str, segment:str, key:str) -> str:
    data_file:str = os.path.join(basedir, dataset_id, segment, f'{key}')

    if not os.path.exists(data_file):
        with open(data_file, mode='a'):
            pass

    return data_file


def _body_to_dict(event) -> Dict[str,str]:
    body: str = event['body']
    key_value: Dict[str, str] = dict()

    # split by new lines, then break each line into key=value pairs
    for lines in body.split('\n'):
        split: List[str] = lines.split('=')

        if len(split) == 2:
            key_value[split[0].lower()] = split[1]

    # perform validation checks for the dictionary to make sure that the minimum
    # required fields are present in the dictionary to allow processing

    if 'dataset-id' not in key_value:
        raise BodyError('No dataset-id specified')

    if 'timestamp' not in key_value:
        raise BodyError('No timestamp specified')

    if len(key_value['timestamp']) == 0:
        raise BodyError('Empty timestamp specified')

    if 'correlation-id' not in key_value:
        raise BodyError('No correlation-id specified')

    if len(key_value['correlation-id']) == 0:
        raise BodyError('Empty correlation-id specified')

    if len(key_value['correlation-id']) > 20:
        raise BodyError('Specified correlation-id is too long')

    return key_value


def _append_record(dataset_id: str, segment_id : str, timestamp:str, correlation_id: str, key :str, value: str):
    path: str = _get_file_path_for_column(__SEGMENT_BASEDIR__, dataset_id, segment_id, key)

    with open(path, 'a') as column_file:
        print(f'Writing value {key},{value} to {path}')

        # append the value to the column file, using fixed-width values for the entries
        column_file.write(f'{timestamp.ljust(20)}{correlation_id.ljust(20)}{value}\n')
        column_file.flush()


class BodyError(Exception):
    pass

