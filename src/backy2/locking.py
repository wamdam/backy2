import psutil
from filelock import FileLock, Timeout


class Locking:
    """ implements name-based locking """
    lock_dir = None

    def __init__(self, lock_dir):
        self._lock_dir = lock_dir
        self._locks = {}  # contains name => FileLock object

    def lock(self, name):
        """
        Lock access to a specific name. Returns True when locking
        was successful or False if this name is already locked.
        """
        self._locks[name] = FileLock('{}/backy_{}.lock'.format(self._lock_dir, name), timeout=0)
        try:
            self._locks[name].acquire()
        except Timeout:
            return False
        else:
            return True

    def unlock(self, name):
        self._locks[name].release()
        del self._locks[name]


def find_other_procs(name):
    """ returns other processes by given name """
    return [p for p in psutil.process_iter() if p.name().split()[0] == name]

