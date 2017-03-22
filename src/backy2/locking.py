import ctypes
import fcntl
import os
import psutil
import sys


class Locking:
    """ implements name-based locking """
    lock_dir = None

    def __init__(self, lock_dir):
        self.lock_dir = lock_dir
        self._locks = {}  # contains name => file descriptor
        self._semaphores = {}


    def _lock(self, name):
        """ Create a lock for a given name.
        Locks will be ignored if the matching process is dead. This means, they
        will be removed automatically when the process dies.
        """
        if not self.lock_dir:
            return True  # i.e. no locking
        lpath = os.path.join(self.lock_dir, name)
        fd = None
        try:
            fd = os.open(lpath, os.O_CREAT)
            fcntl.flock(fd, fcntl.LOCK_NB | fcntl.LOCK_EX)
            self._locks[name] = fd
            return True
        except (OSError, IOError):
            if fd: os.close(fd)
            return False


    def _unlock(self, name):
        if not self.lock_dir:
            return True  # i.e. no locking
        if name not in self._locks:
            return True  # nothing to unlock
        os.close(self._locks[name])
        return True


    def lock(self, name):
        """
        Lock access to a specific name. Returns True when locking
        was successful or False if this name is already locked.
        """
        return self._lock('backy_{}.lock'.format(name))


    def unlock(self, name):
        return self._unlock('backy_{}.lock'.format(name))


def find_other_procs(name):
    """ returns other processes by given name """
    return [p for p in psutil.process_iter() if p.name().split()[0] == name]

