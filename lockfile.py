
__all__ = ['lockfile']

import fcntl
import logging
import pathlib
import time

log = logging.getLogger(__name__)

class lockfile:
    
    __slots__ = ('path', 'file', '_locked')

    def __init__(self, path):
        assert isinstance(path, pathlib.Path)
        self._locked = False
        self.path = path
        self.file = None

    def acquire(self, block=True, timeout=None):
        '''acquire lock

        If optional args 'block' is true and 'timeout' is None (the default),
        block until succesful lock. If 'timeout' is a non-negative number, 
        it blocks at most 'timeout' seconds and raises the BlockingIOError exception 
        if no lock was succesful within that time.

        Otherwise ('block' is false), acquire lock if it is immediately available, 
        else raise the BlockingIOError exception ('timeout' is ignored in that case).
        '''
        if timeout is not None and timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")
        timeout_ns = timeout * 1_000_000_000 if timeout is not None else None
        point_0_ns = time.monotonic_ns()
        if self._locked:
            raise RuntimeError(f'unexpected acquire() call, lockfile {self.path} already in the locked state')
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        while True:
            try:
                self.file = self.path.open('a+b')
                fcntl.flock(self.file, fcntl.LOCK_EX|fcntl.LOCK_NB)
                self._locked = True
                log.debug(f'success acquire lock {self.path}')
                return
            except BlockingIOError:
                if self.file is not None:
                    try:
                        self.file.close()
                        self.file = None
                    except Exception:
                        pass
                if not block:
                    log.debug(f'failed acquire lock {self.path} - raise BlockingIOError, because block=False')
                    raise
                elif timeout is None:
                    log.debug(f'failed acquire lock {self.path} - wait and try again')
                    time.sleep(1)
                    continue
                elif time.monotonic_ns() < point_0_ns + timeout_ns:
                    log.debug(f'failed acquire lock {self.path} - wait and try again')
                    time.sleep(1)
                    continue
                else:
                    log.debug(f'failed acquire lock {self.path} - raise BlockingIOError after timeout')
                    raise

    def release(self):
        if not self._locked:
            raise RuntimeError(f'unexpected release() call, lockfile {self.path} already in the unlocked state')
        fcntl.flock(self.file, fcntl.LOCK_UN)
        log.debug(f'released lock {self.path}')
        try:
            self.file.close()
        except Exception:
            pass
        self.file = None
        self._locked = False

    def locked(self):
        return self._locked

    def __enter__(self):
        self.acquire()
        assert self.locked()

    def __exit__(self, exception_type, exception_value, traceback):
        self.release()
        assert not self.locked()

