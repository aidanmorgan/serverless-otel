# This is a basic attempt to use a lock file as a mechanism for controlling write access to
# the files that are handled by the ingest lambda.
#
# The idea here is that the lambda has an EFS (which is NFS) file share mounted when it runs that
# allows the file system to be shared amongst all lambda instances.
#
# According to some research, appends to files are considered atomic when they are on the local filesystem
# (and under a certain size), but are not atomic when operating over the network. HOWEVER, it appears that
# linking and unlinking a symlink IS atomic across the network.
#
# Obviously appends are one mechanism we are testing, if we're using SQLite, for example, we can't append we need
# to have a mutex to control only one reader at a time
#
# To use this as a mutex, the following is done:
#   A directory is created (.locks)
#   A 0-bytes file for each instance that may want to hold the lock is created
#
# When a lambda requests the lock:
#   Attempt to link the instance id lock file to the "segment lock file" - that is the lock file that is the thing that
#   actually controls access to the file system.
#   If the link is successful - the lock is held, if not then backoff and try again to a maximim time
#
# When a lambda requests an unlock:
#   Read the "segment lock file"
#   If it is linked to the "instance lock file" that is our instance then attempt to unlink symlink
#   If it's not linked then we are attempting to unlock a link we don't own

import errno
import os
import time
from datetime import timedelta
from typing import Callable

from lru import LRUCache
from lru.CacheStorage import ItemNotCached
from opentelemetry import trace
from opentelemetry.trace import StatusCode, Span

from serverless_otel.common.constants import *
from serverless_otel.common.errors import SegmentLockError, SegmentUnlockError

LOCK_DIRECTORY: Final[str] = '.locks'
LRU_CACHE_SIZE: Final[int] = 50

NfsLockIdentifier = namedtuple('NfsLockIdentifier', ['lockfile', 'timestamp'])

InitialisationKey = namedtuple('InitialisationKey', ['dataset_id', 'instance', 'segment'])
initialisation_cache: LRUCache = LRUCache(max_age=timedelta(minutes=15))

fs_tracer = trace.get_tracer(__name__)


def _get_segment_lockdir(basedir: str, segment: str) -> str:
    return os.path.join(basedir, segment, LOCK_DIRECTORY)


def _get_segment_lockfile(basedir: str, segment: str) -> str:
    return os.path.join(_get_segment_lockdir(basedir, segment), f'{segment}.lck')


def _get_instance_lockfile(basedir: str, segment: str, instance: str) -> str:
    return os.path.join(_get_segment_lockdir(basedir, segment), f'{instance}.lck')


# ensures that the required files and directories are in place for the segment locking approach
# to work appropriately
@fs_tracer.start_as_current_span("nfs_initialise_segment_locks")
def nfs_initialise_segment_locks(basedir: str, dataset_id: str, instance: str, segment: str):
    span: Span = trace.get_current_span()
    key: InitialisationKey = InitialisationKey(dataset_id, instance, segment)

    # check the cache to see if we have already initialised previously, that way
    # we don't need to waste time with additional OS calls
    try:
        exists: bool = initialisation_cache[key]
        span.set_attribute('segment_lockpath_cached', 'True')

        return
    except ItemNotCached:
        span.set_attribute('segment_lockpath_cached', 'False')
        pass

    dataset_base: str = os.path.join(basedir, dataset_id)

    segment_lockpath: str = _get_segment_lockdir(dataset_base, segment)
    segment_lockfile: str = _get_segment_lockfile(dataset_base, segment)

    span.set_attribute(f'segment_lockpath', segment_lockpath)
    span.set_attribute(f'segment_lockfile', segment_lockfile)
    span.set_attribute(f'segment_lockpath_exists', os.path.exists(segment_lockpath))

    if not os.path.exists(segment_lockpath):
        os.makedirs(segment_lockpath)

    instance_lockfile: str = _get_instance_lockfile(dataset_base, segment, instance)
    span.set_attribute(f'instance_lockfile', instance_lockfile)
    span.set_attribute(f'instance_lockfile_exists', os.path.exists(instance_lockfile))

    if not os.path.exists(instance_lockfile):
        with open(instance_lockfile, mode='a'):
            pass

    initialisation_cache[key] = True


# attempts to get an exclusive lock on the directory for the dataset and segment, it does this by
# repeatedly attempting to create a symlink between the instance lock file and the directory lock file
# as this is MEANT to be an atomic operation on a NFS file share.
@fs_tracer.start_as_current_span("nfs_lock_segment")
def nfs_lock_segment(basedir: str, dataset_id: str, segment: str, instance: str, utc_nanos: Callable[[], int],
                     timeout: int = 10, delay: int = 1) -> NfsLockIdentifier:
    span: Span = trace.get_current_span()
    dataset_base: str = os.path.join(basedir, dataset_id)

    segment_lockfile: str = _get_segment_lockfile(dataset_base, segment)
    instance_lockfile: str = _get_instance_lockfile(dataset_base, segment, instance)

    span.set_attribute('segment_lockfile', segment_lockfile)
    span.set_attribute('instance_lockfile', instance_lockfile)

    start: int = utc_nanos()
    retry_count: int = 0

    # this is primitive, but it will work for these experiments, really need to replace with a proper
    # bulkhead and exponential backoff approach. Probably should also consider having two NFS mounts available
    # and swapping between them where required? has some interesting challenges with detecting that failure mode
    while (utc_nanos() - start) < (timeout * NS_PER_MIN):
        span.set_attribute('tries', (retry_count + 1))
        try:
            os.symlink(instance_lockfile, segment_lockfile)
            span.set_status(StatusCode.OK)
            return NfsLockIdentifier(lockfile=instance_lockfile, timestamp=utc_nanos())
        except OSError as err:
            if err.errno == errno.EEXIST:
                time.sleep(delay)
            else:
                span.set_status(StatusCode.ERROR, str(err))
                raise SegmentLockError(f'Unexpected error locking {segment_lockfile}. {err}')
        except Exception as err:
            span.set_status(StatusCode.ERROR, str(err))
            raise SegmentLockError(f'Unexpected error locking {segment_lockfile}. {err}')
        finally:
            retry_count += 1

    span.set_status(StatusCode.ERROR, 'Timeout')
    raise SegmentLockError(f'Failed to lock segment {segment_lockfile}, timed out.')


# attempts to release the exclusive lock for the dataset and segment, it does this by
# validating that the symlink is pointing to the relevant instance lock file and then
# unlinking the file if the lock is present
@fs_tracer.start_as_current_span("nfs_unlock_segment")
def nfs_unlock_segment(basedir: str, dataset_id: str, segment: str, instance: str, lock: NfsLockIdentifier):
    if lock is None:
        raise SegmentUnlockError(f'Cannot unlock segment, no lock held.')

    span: Span = trace.get_current_span()
    dataset_base: str = os.path.join(basedir, dataset_id)

    instance_lockfile: str = _get_instance_lockfile(dataset_base, segment, instance)
    segment_lockfile: str = _get_segment_lockfile(dataset_base, segment)

    span.set_attribute('segment_lockfile', segment_lockfile)
    span.set_attribute('instance_lockfile', instance_lockfile)

    if instance_lockfile != lock.lockfile:
        span.set_status(StatusCode.ERROR,
                        f'Unlock failed, owned by {lock.lockfile} but requested by {instance_lockfile}')
        raise SegmentUnlockError(f'Cannot unlock segment {segment_lockfile}, it is owned by {lock.lockfile}')

    current_lock: str = os.readlink(segment_lockfile)

    if current_lock != instance_lockfile:
        span.set_status(StatusCode.ERROR,
                        f'Unlock failed, owned by {current_lock} but requested by {instance_lockfile}')
        raise SegmentUnlockError(f'Cannot unlock segment {segment_lockfile}, it is owned by {current_lock}')
    else:
        try:
            os.unlink(segment_lockfile)
            span.set_status(StatusCode.OK)
        except OSError as err:
            span.set_status(StatusCode.ERROR, str(err))
            raise SegmentUnlockError(f'Cannot unlock segment, probably a deadlock. {err}')
        except Exception as err:
            span.set_status(StatusCode.ERROR, str(err))
            raise SegmentUnlockError(f'Cannot unlock segment, unexpected exception. {err}')
