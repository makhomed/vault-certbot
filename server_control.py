#!/usr/bin/python3 -u

import fabric
import io
import pprint
import secrets
import sys
import time
import uuid
import yaml
import os.path

from pathlib import Path
from temporalio.exceptions import ApplicationError

def fatal(*args):
    raise ApplicationError(*args, non_retryable=True)

system_ssh_connect_timeout = 10
system_ssh_banner_timeout = 10
system_ssh_auth_timeout = 10

system_ssh_execute_timeout = 900
system_ssh_reboot_timeout = 600

class ServerControl:

    def __init__(self, connection_host, connection_user='root'):
        self._connection_host = connection_host
        self._connection_user = connection_user
        self._connection = None

    @property
    def connection_host(self):
        return self._connection_host

    @property
    def connection_user(self):
        return self._connection_user

    @property
    def connection_port(self):
        return 22

    @property
    def connection(self):
        if self._connection is None:
            connect_timeout = system_ssh_connect_timeout
            connect_kwargs = { 'banner_timeout': system_ssh_banner_timeout, 'auth_timeout': system_ssh_auth_timeout }
            self._connection = fabric.Connection(self.connection_host, user=self.connection_user, port=self.connection_port,
                                                        connect_timeout=connect_timeout, connect_kwargs=connect_kwargs)
        return self._connection

    def close_connection(self):
        try:
            self._connection.close()
        except Exception:
            pass
        finally:
            self._connection = None

    def run(self, *args, **kwargs):
        if 'timeout' not in kwargs:
            kwargs['timeout'] = system_ssh_execute_timeout
        if 'hide' not in kwargs:
            kwargs['hide'] = True
        return self.connection.run(*args, **kwargs)

    def is_user_exists(self, user):
        return self.run(f"getent passwd {user}", warn=True, hide=True).stdout.strip().startswith(f"{user}:")

    def mountpoints(self):
        return self.run('findmnt -n -l -o TARGET', hide=True).stdout.strip().splitlines()

    def is_exists(self, remote_filename):
        return self.run(f"""if [ -e {remote_filename} ]; then echo "True"; fi""", hide=True).stdout.strip() == "True"

    def is_dir(self, remote_filename):
        return self.run(f"""if [ -d {remote_filename} ]; then echo "True"; fi""", hide=True).stdout.strip() == "True"

    def is_empty_dir(self, remote_filename):
        return self.run(f"""if [ -d  {remote_filename} ] && [ -z $(ls -A  {remote_filename}) ] ; then echo "True" ; fi""", hide=True).stdout.strip() == "True"

    def is_file(self, remote_filename):
        return self.run(f"""if [ -f {remote_filename} ]; then echo "True"; fi""", hide=True).stdout.strip() == "True"

    def is_executable(self, remote_filename):
        return self.run(f"""if [ -x {remote_filename} ]; then echo "True"; fi""", hide=True).stdout.strip() == "True"

    def is_symlink(self, remote_filename):
        return self.run(f"""if [ -L {remote_filename} ]; then echo "True"; fi""", hide=True).stdout.strip() == "True"

    def is_socket(self, remote_filename):
        return self.run(f"""if [ -S {remote_filename} ]; then echo "True"; fi""", hide=True).stdout.strip() == "True"

    def is_block_device(self, remote_filename):
        return self.run(f"""if [ -b {remote_filename} ]; then echo "True"; fi""", hide=True).stdout.strip() == "True"

    def is_char_device(self, remote_filename):
        return self.run(f"""if [ -c {remote_filename} ]; then echo "True"; fi""", hide=True).stdout.strip() == "True"

    def run(self, *args, **kwargs):
        return self.connection.run(*args, **kwargs)

    def copy(self, local_filename, remote_filename, *, mode=None, user=None, group=None):
        if group is None:
            group = user
        if user is not None:
            uid = int(self.run(f"id -u {user}", hide=True).stdout.strip())
            gid = int(self.run(f"id -g {group}", hide=True).stdout.strip())
        else:
            uid = None
            gid = None
        if not Path(local_filename).is_absolute():
            fatal(f"Local filename must be absolute: {local_filename}")
        local_filename_path = Path(local_filename)
        local_filename = str(local_filename_path)
        if local_filename_path.is_symlink():
            fatal(f"Local filename must be regular file: {local_filename}")
        remote_filename_path = Path(remote_filename)
        if not remote_filename_path.is_absolute():
            fatal(f"Remote filename must be absolute: {remote_filename}")
        if self.is_file(remote_filename):
            local_stat = local_filename_path.lstat()
            if mode is None:
                mode = local_stat.st_mode & 0o777
            else:
                if not isinstance(mode, int) or mode < 0o000 or mode > 0o777:
                    fatal(f"invalid mode: '{oct(mode)}'")
            remote_stat = self.connection.sftp().lstat(remote_filename)
            if local_stat.st_size == remote_stat.st_size:
                local_content = local_filename_path.read_bytes()
                remote_content = io.BytesIO()
                self.connection.sftp().getfo(remote_filename, remote_content)
                remote_content = remote_content.getvalue()
                if local_content == remote_content:
                    changed1 = False
                    changed2 = False
                    if mode != (remote_stat.st_mode & 0o777):
                        self.connection.sftp().chmod(remote_filename, mode)
                        changed1 = True
                    if (uid is not None and gid is not None) and (remote_stat.st_uid != uid or remote_stat.st_gid != gid):
                        self.connection.sftp().chown(remote_filename, uid, gid)
                        changed2 = True
                    return changed1 or changed2
        tmp_filename = str(remote_filename_path.with_name(remote_filename_path.name + '.tmp.' + uuid.uuid4().hex + '.tmp'))
        self.connection.sftp().put(local_filename, tmp_filename)
        self.connection.sftp().chmod(tmp_filename, mode)
        if (uid is not None and gid is not None):
            self.connection.sftp().chown(tmp_filename, uid, gid)
        self.connection.sftp().posix_rename(tmp_filename, remote_filename)
        return True

    def put(self, content, remote_filename, *, mode=0o644, user=None, group=None):
        if group is None:
            group = user
        if user is not None:
            uid = int(self.run(f"id -u {user}", hide=True).stdout.strip())
            gid = int(self.run(f"id -g {group}", hide=True).stdout.strip())
        else:
            uid = None
            gid = None
        if not isinstance(mode, int) or mode < 0o000 or mode > 0o777:
            fatal(f"invalid mode: '{oct(mode)}'")
        remote_filename_path = Path(remote_filename)
        remote_filename = str(remote_filename_path)
        if not remote_filename_path.is_absolute():
            fatal(f"Remote filename must be absolute: {remote_filename}")
        if self.is_file(remote_filename):
            local_content = bytes(content, encoding='utf-8', errors='strict')
            local_size = len(local_content)
            remote_stat = self.connection.sftp().lstat(remote_filename)
            if local_size == remote_stat.st_size:
                remote_content = io.BytesIO()
                self.connection.sftp().getfo(remote_filename, remote_content)
                remote_content = remote_content.getvalue()
                if local_content == remote_content:
                    changed1 = False
                    changed2 = False
                    if mode != (remote_stat.st_mode & 0o777):
                        self.connection.sftp().chmod(remote_filename, mode)
                        changed1 = True
                    if (uid is not None and gid is not None) and (remote_stat.st_uid != uid or remote_stat.st_gid != gid):
                        self.connection.sftp().chown(remote_filename, uid, gid)
                        changed2 = True
                    return changed1 or changed2
        tmp_filename = str(remote_filename_path.with_name(remote_filename_path.name + '.tmp.' + uuid.uuid4().hex + '.tmp'))
        self.connection.sftp().putfo(io.StringIO(content), tmp_filename)
        self.connection.sftp().chmod(tmp_filename, mode)
        if (uid is not None and gid is not None):
            self.connection.sftp().chown(tmp_filename, uid, gid)
        self.connection.sftp().posix_rename(tmp_filename, remote_filename)
        return True

    def get(self, remote_filename, *, encoding='utf-8', errors='strict'):
        """ Call with encoding=None to obtain raw bytes
        """
        remote_filename_path = Path(remote_filename)
        if not remote_filename_path.is_absolute():
            fatal(f"Remote filename must be absolute: {remote_filename}")
        if not self.is_file(remote_filename):
            fatal(f"Remote file '{remote_filename}' not exists")
        remote_content = io.BytesIO()
        self.connection.sftp().getfo(remote_filename, remote_content)
        remote_content = remote_content.getvalue()
        if encoding is None:
            return remote_content
        else:
            return remote_content.decode(encoding, errors)

    def unlink(self, remote_filename):
        return self.remove(remote_filename)

    def remove(self, remote_filename):
        remote_filename_path = Path(remote_filename)
        if not remote_filename_path.is_absolute():
            fatal(f"Remote filename must be absolute: {remote_filename}")
        if not self.is_exists(remote_filename):
            changed = False
            return changed
        if self.is_file(remote_filename):
            self.connection.sftp().remove(remote_filename)
            changed = True
            return changed
        fatal(f"Remote file '{remote_filename}' exists, but it is not regular file")

    def mkdir(self, remote_dirname, *, mode=None, parents=False, owner=None, group=None):
        if not Path(remote_dirname).is_absolute():
            raise RuntimeError(f"Remote dirname must be absolute: {remote_dirname}")
        if mode is not None:
            if not isinstance(mode, int):
                raise RuntimeError(f"invalid mode: '{mode}'")
            if mode < 0o000 or mode > 0o777:
                raise RuntimeError(f"invalid mode: '{mode:04o}'")
        mode_param = f"-m{mode:04o}" if mode is not None else ""
        parents_param = "--parents" if parents else ""
        params = f"{parents_param} {mode_param}"
        changed = self.run(f'if [ ! -d {remote_dirname} ] ; then mkdir {params} -- {remote_dirname} ; echo created ; fi', hide=True).stdout == 'created'
        if owner is not None and group is not None:
            self.run(f"chown {owner.strip()}:{group.strip()} -- {remote_dirname}").stdout.strip()
        return changed

    def rmdir(self, remote_dirname):
        remote_dirname_path = Path(remote_dirname)
        if not remote_dirname_path.is_absolute():
            fatal(f"Remote dirname must be absolute: {remote_dirname}")
        if not self.is_exists(remote_dirname):
            changed = False
            return changed
        if self.is_dir(remote_dirname):
            self.connection.sftp().rmdir(remote_dirname)
            changed = True
            return changed
        fatal(f"Remote file '{remote_dirname}' exists, but it is not directory")

    def chown(self, remote_filename, *, owner, group):
        if not os.path.isabs(remote_filename):
            raise RuntimeError(f"Remote filename must be absolute: {remote_filename}")
        stdout = self.run(f"chown --changes {owner.strip()}:{group.strip()} -- {remote_filename}").stdout.strip()
        changed = stdout != ""
        return changed

    def chmod(self, remote_filename, *, mode):
        if not isinstance(mode, int):
            raise RuntimeError(f"invalid mode: '{mode}'")
        if mode < 0o000 or mode > 0o777:
            raise RuntimeError(f"invalid mode: '{mode:04o}'")
        if not os.path.isabs(remote_filename):
            raise RuntimeError(f"Remote filename must be absolute: {remote_filename}")
        stdout = self.run(f"chmod --changes {mode:04o} -- {remote_filename}").stdout.strip()
        changed = stdout != ""
        return changed

if __name__=='__main__':
    ctx = ServerControl('127.0.0.1')

