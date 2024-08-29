# This is a basic attempt to use a lock file as a mechanism for controlling write access to
# the files that are handled by the ingest lambda.
#
# The idea here is that the lambda has a EFS (which is NFS) file share mounted when it runs that
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
# A directory is created (.locks)
# A 0-bytes file for each instance that may want to hold the lock is created
#
# When a lambda requests the lock:
# Attempt to link the instance id lock file to the "segment lock file" - that is the lock file that is the thing that
# actually controls access to the file system.
# If the link is successful - the lock is held, if not then backoff and try again to a maximim time
#
# When a lambda requests an unlock:
# Read the "segment lock file"
# If it is linked to the "instance lock file" that is our instance then attempt to unlink symlink
# If it's not linked then we are attempting to unlock a link we don't own

import errno
import os
import time
from collections import namedtuple
from typing import Callable
from constants import *
from mutex import SegmentLockError, SegmentUnlockError

__LOCK_DIRECTORY__: Final[str] = '.locks'

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
# as this is MEANT to be an atomic operation on a NFS file share.
def nfs_lock_segment(basedir: str, dataset_id: str, segment: str, instance: str, utc_nanos: Callable[[], int],
                 timeout: int = 10, delay: int = 1) -> NfsLockIdentifier:
    dataset_base: str = os.path.join(basedir, dataset_id)

    segment_lockfile: str = _get_segment_lockfile(dataset_base, segment)
    instance_lockfile: str = _get_instance_lockfile(dataset_base, segment, instance)

    start: int = utc_nanos()

    # this is primitive, but it will work for these experiments, really need to replace with a proper
    # bulkhead and exponential backoff approach. Probably should also consider having two NFS mounts available
    # and swapping between them where required? has some interesting challenges with detecting that failure mode
    while (utc_nanos() - start) < (timeout * NS_PER_MIN):
        try:
            os.symlink(instance_lockfile, segment_lockfile)
            return NfsLockIdentifier(Lockfile=instance_lockfile,Timestamp=utc_nanos())
        except OSError as err:
            if err.errno == errno.EEXIST:
                time.sleep(delay)
            else:
                raise SegmentLockError(f'Unexpected error locking {segment_lockfile}. {err}')
        except Exception as err:
            raise SegmentLockError(f'Unexpected error locking {segment_lockfile}. {err}')

    raise SegmentLockError(f'Failed to lock segment {segment_lockfile}, timed out.')


# attempts to release the exclusive lock for the dataset and segment, it does this by
# validating that the symlink is pointing to the relevant instance lock file and then
# unlinking the file if the lock is present
def nfs_unlock_segment(basedir: str, dataset_id: str, segment: str, instance: str, lock: NfsLockIdentifier):
    dataset_base: str = os.path.join(basedir, dataset_id)

    instance_lockfile: str = _get_instance_lockfile(dataset_base, segment, instance)
    segment_lockfile: str = _get_segment_lockfile(dataset_base, segment)

    if instance_lockfile != lock.Lockfile:
        raise SegmentUnlockError(f'Cannot unlock segment {segment_lockfile}, it is owned by {lock.Lockfile}')

    current_lock: str = os.readlink(segment_lockfile)

    if current_lock != instance_lockfile:
        raise SegmentUnlockError(f'Cannot unlock segment {segment_lockfile}, it is owned by {current_lock}')
    else:
        try:
            os.unlink(segment_lockfile)
        except OSError as err:
            raise SegmentUnlockError(f'Cannot unlock segment, probably a deadlock. {err}')
        except Exception as err:
            raise SegmentUnlockError(f'Cannot unlock segment, unexpected exception. {err}')

