import errno
import os
import time
from collections import namedtuple
from typing import Callable

from constants import *
from mutex import SegmentLockError, SegmentUnlockError

__LOCK_DIRECTORY__: str = '.locks'

NfsLockIdentifier = namedtuple('NfsLockIdentifier', ['Lockfile', 'Timestamp'])

def _get_segment_lockdir(basedir: str, segment: str) -> str:
    return os.path.join(basedir, segment, __LOCK_DIRECTORY__)


def _get_segment_lockfile(basedir: str, segment: str) -> str:
    return os.path.join(_get_segment_lockdir(basedir, segment), f'{segment}.lck')


def _get_instance_lockfile(basedir: str, segment: str, instance: str) -> str:
    return os.path.join(_get_segment_lockdir(basedir, segment), f'{instance}.lck')


# ensures that the required files and directories are in place for the segment locking approach
# to work appropriately
def nfs_initialise_segment_locks(basedir: str, dataset_id: str, instance: str, segment: str):
    dataset_base: str = os.path.join(basedir, dataset_id)

    segment_lockpath: str = _get_segment_lockdir(dataset_base, segment)
    segment_lockfile: str = _get_segment_lockfile(dataset_base, segment)

    if not os.path.exists(segment_lockpath):
        os.makedirs(segment_lockpath)

    instance_lockfile: str = _get_instance_lockfile(dataset_base, segment, instance)
    if not os.path.exists(instance_lockfile):
        with open(instance_lockfile, mode='a'):
            pass


# attempts to get an exclusive lock on the directory for the dataset and segment, it does this by
# repeatedly attempting to create a symlink between the instance lock file and the directory lock file
# as this is MEANT to be an atomic operation on a NFS file share
def nfs_lock_segment(basedir: str, dataset_id: str, segment: str, instance: str, utc_nanos: Callable[[], int],
                 timeout: int = 300, delay: int = 5) -> NfsLockIdentifier:
    dataset_base: str = os.path.join(basedir, dataset_id)

    segment_lockfile: str = _get_segment_lockfile(dataset_base, segment)
    instance_lockfile: str = _get_instance_lockfile(dataset_base, segment, instance)

    start: int = utc_nanos()

    while (utc_nanos() - start) < (timeout * NS_PER_MIN):
        try:
            os.symlink(instance_lockfile, segment_lockfile)
            return NfsLockIdentifier(Lockfile=instance_lockfile,Timestamp=utc_nanos())
        except OSError as err:
            if err.errno == errno.EEXIST:
                time.sleep(delay)

    raise SegmentLockError('Cannot lock segment')


# attempts to release the exclusive lock for the dataset and segment, it does this by
# validating that the symlink is pointing to the relevant instance lock file and then
# unlinking the file if the lock is present
def nfs_unlock_segment(basedir: str, dataset_id: str, segment: str, instance: str, lock: NfsLockIdentifier):
    dataset_base: str = os.path.join(basedir, dataset_id)

    segment_lockfile: str = _get_segment_lockfile(dataset_base, segment)
    instance_lockfile: str = _get_instance_lockfile(dataset_base, segment, instance)

    if instance_lockfile != lock.Lockfile:
        raise SegmentUnlockError('Cannot unlock segment')

    current_lock: str = os.readlink(segment_lockfile)

    if current_lock != instance_lockfile:
        raise SegmentUnlockError('Cannot unlock segment, not owned by instance')
    else:
        try:
            os.unlink(segment_lockfile)
        except OSError as err:
            raise SegmentUnlockError(f'Cannot unlock segment, probably a deadlock. {err}')


