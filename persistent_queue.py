
__all__ = ['Empty', 'Full', 'Queue']

"""A thread-safe disk based persistent queue."""


from pathlib import Path

import json
import logging
import re
import secrets
import time

import lockfile

log = logging.getLogger(__name__)

def atomic_write_text(content, filename): # {{{
    assert isinstance(content, str)
    assert isinstance(filename, Path)
    tmp_filename = filename.with_name(filename.name + '.tmp.' + secrets.token_hex() + '.tmp')
    tmp_filename.write_text(content)
    tmp_filename.rename(filename)

# }}}

class Empty(Exception): # {{{
    pass
# }}}

class Full(Exception): # {{{
    pass
# }}}

META_INFO = r':(?P<head>\d+):(?P<tail>\d+):(?P<size>\d+):(?P<maxsize>\d+):'
PRECISION = 20 # len( str( 2 ** 64 - 1 ) )
LAST_TAIL = 2 ** 64 - 1

class Queue:

    def __init__(self, path, maxsize): # {{{
        assert isinstance(maxsize, int)
        if maxsize < 0: 
            maxsize = 0
        if maxsize > LAST_TAIL: 
            raise ValueError(f'maxsize {maxsize} must me less then {LAST_TAIL}')
        self.maxsize = maxsize
        assert isinstance(path, Path)
        self.path = Path(path)
        if not self.path.is_dir(): 
            self.path.mkdir(mode=0o700, parents=True, exist_ok=True)
        assert self.path.is_dir()
        self.metadata_filename = self.path / '.metadata'
        self.readhead_filename = None
        self.metadata_lock = lockfile.lockfile( self.path / '.metadata-lockfile' )
        self.readhead_lock = lockfile.lockfile( self.path / '.readhead-lockfile' )
        with self.metadata_lock:
            head, tail, size, maxsize = self._get_metadata()
            self._put_metadata(head, tail, size, maxsize)
    # }}}

    def _get_metadata(self): # {{{
        content = self.metadata_filename.read_text() if self.metadata_filename.is_file() else ''
        if match := re.fullmatch(META_INFO, content):
            head = int(match.group('head'))
            tail = int(match.group('tail'))
            size = int(match.group('size'))
            maxsize = int(match.group('maxsize'))
            if maxsize != self.maxsize:
                raise RuntimeError(f'unexpected queue maxsize {maxsize}, expected {self.maxsize}')
            return head, tail, size, maxsize
        return 0, 0, 0, self.maxsize
    # }}}

    def peek(self, maxpeek=0): # {{{
        """ peek queue without lock
        """
        out = list()
        head, tail, size, maxsize = self._get_metadata()
        assert tail - head == size
        if maxpeek > 0:
            peek_maxsize = min(maxpeek, size)
        else:
            peek_maxsize = size + 20
        peek_size = 0
        peek_head = head
        while True:
            peek_head_filename = Path(self.path / f'{peek_head:020d}')
            if peek_head_filename.is_file():
                try:
                    content = peek_head_filename.read_text()
                    if content:
                        item = json.loads(content)
                        out.append(item)
                except Exception:
                    pass
            peek_size += 1
            peek_head += 1
            if peek_head > LAST_TAIL: 
                peek_head = 0
            if peek_size > peek_maxsize:
                break
        return out
    # }}}

    def _put_metadata(self, head, tail, size, maxsize): # {{{
        assert isinstance(head, int) and head >= 0
        assert isinstance(tail, int) and tail >= 0
        assert isinstance(size, int) and size >= 0
        assert isinstance(maxsize, int) and maxsize >= 0
        if maxsize != self.maxsize:
            raise RuntimeError(f'unexpected queue maxsize {maxsize}, expected {self.maxsize}')
        atomic_write_text(f':{head:020d}:{tail:020d}:{size:020d}:{maxsize:020d}:', self.metadata_filename)
    # }}}

    def put(self, item, block=True, timeout=None): # {{{
        '''Put an item into the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until a free slot is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Full exception if no free slot was available within that time.
        Otherwise ('block' is false), put an item on the queue if a free slot
        is immediately available, else raise the Full exception ('timeout'
        is ignored in that case).
        '''
        if timeout is not None and timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")
        timeout_ns = timeout * 1_000_000_000 if timeout is not None else None
        point_0_ns = time.monotonic_ns()
        while True:
            try:
                with self.metadata_lock:
                    head, tail, size, maxsize = self._get_metadata()
                    if maxsize != self.maxsize:
                        raise RuntimeError(f'unexpected queue maxsize {maxsize}, expected {self.maxsize}')
                    if self.maxsize > 0 and size >= self.maxsize:
                        raise Full
                    new_tail_filename = Path(self.path / f'{tail:020d}')
                    _task = str(tail)
                    assert isinstance(item, dict)
                    assert '_task' not in item
                    item['_task'] = _task
                    content = json.dumps(item, indent=4, ensure_ascii=False, sort_keys=True)
                    atomic_write_text(content, new_tail_filename)
                    tail += 1
                    if tail > LAST_TAIL: 
                        tail = 0
                    size += 1
                    self._put_metadata(head, tail, size, maxsize)
                    return _task
            except Full:
                if not block:
                    raise
                elif timeout is None:
                    time.sleep(1)
                    continue
                elif time.monotonic_ns() < point_0_ns + timeout_ns:
                    time.sleep(1)
                    continue
                else:
                    raise 
    # }}}

    def get(self, block=True, timeout=None): # {{{
        '''Remove and return an item from the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until an item is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Empty exception if no item was available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).
        '''
        if self.readhead_lock.locked():
            raise RuntimeError(f'unexpected get() call, queue already locked by get() call, now task_done() call expected')

        if timeout is not None and timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")

        timeout_ns = timeout * 1_000_000_000 if timeout is not None else None
        point_0_ns = time.monotonic_ns()
        while True:
            try:
                with self.metadata_lock:
                    head, tail, size, maxsize = self._get_metadata()
                    if size == 0:
                        raise Empty
                    self.readhead_filename = Path(self.path / f'{head:020d}')
                    try:
                        self.readhead_lock.acquire(block=False)
                    except BlockingIOError:
                        raise Empty
                    item = json.loads(self.readhead_filename.read_text())
                    return item # but held lock and need to call task_done()
            except Empty:
                if not block:
                    raise
                elif timeout is None:
                    time.sleep(1)
                    continue
                elif time.monotonic_ns() < point_0_ns + timeout_ns:
                    time.sleep(1)
                    continue
                else:
                    raise
# }}}

    def task_done(self): # {{{
        '''Remove and return an item from the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until an item is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Empty exception if no item was available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).
        '''
        if not self.readhead_lock.locked():
            raise RuntimeError(f'unexpected task_done() call, because queue not locked by get() call')
        if not self.readhead_filename.is_file(): 
            raise RuntimeError(f'unexpected internal state, head file {self.readhead_filename} not exists')
        with self.metadata_lock:
            head, tail, size, maxsize = self._get_metadata()
            assert size > 0
            self.readhead_filename.unlink()
            size -= 1
            head += 1
            if head > LAST_TAIL: 
                head = 0
            self._put_metadata(head, tail, size, maxsize)
            self.readhead_lock.release()
            self.readhead_filename = None
    # }}}

    def put_nowait(self, item): # {{{
        '''Put an item into the queue without blocking.

        Only enqueue the item if a free slot is immediately available.
        Otherwise raise the Full exception.
        '''
        return self.put(item, block=False)
    # }}}

    def get_nowait(self): # {{{
        '''Remove and return an item from the queue without blocking.

        Only get an item if one is immediately available. Otherwise
        raise the Empty exception.
        '''
        return self.get(block=False)
    # }}}

    def join(self): # {{{
        '''Blocks until all items in the queue have been gotten and processed.

        The count of unfinished tasks goes up whenever an item is added to the
        queue. The count goes down whenever a consumer thread calls task_done()
        to indicate the item was retrieved and all work on it is complete.

        When the count of unfinished tasks drops to zero, join() unblocks.
        '''
        while True:
            with self.metadata_lock:
                head, tail, size, maxsize = self._get_metadata()
                if size == 0:
                    return
            time.sleep(1)
    # }}}

