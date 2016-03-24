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


# these are used for locking more or less because backy looks for its own process
# name

def setprocname(name):
    """ Set own process name """
    if sys.platform not in ('linux', 'linux2'):
        # XXX: Would this work in bsd or so too similarly?
        raise RuntimeError("Unable to set procname when not in linux.")
    if type(name) is not bytes:
        name = str(name)
        name = bytes(name.encode("utf-8"))
    libc = ctypes.cdll.LoadLibrary('libc.so.6')
    return libc.prctl(15, name, 0, 0, 0)  # 15 = PR_SET_NAME, returns 0 on success.


def getprocname():
    return sys.argv[0]


def find_other_procs(name):
    """ returns other processes by given name """
    return [p for p in psutil.process_iter() if p.name() == name]

