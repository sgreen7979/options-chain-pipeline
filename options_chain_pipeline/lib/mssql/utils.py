#!/usr/bin/env python3
from abc import abstractmethod
import os
from types import TracebackType
from typing import List
from typing import Optional
from typing import TypeVar
import uuid
import zlib

import paramiko

from options_chain_pipeline.lib.types import StrPath
from options_chain_pipeline.lib.utils.networking import get_hostname
from options_chain_pipeline.lib.utils.logging import get_logger

from .connection_string import ConnString

logger = get_logger("TempHandler", propagate=False, fh=True)

T = TypeVar("T")


class AbstractTempHandler:

    def __init__(self):
        self.logger = logger

    @abstractmethod
    def write(self, data: List[str]) -> str: ...

    def __enter__(self: T) -> T: ...

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None: ...


class RemoteTempHandler(AbstractTempHandler):
    def __init__(
        self,
        hostname: str,
        username: str,
        password: str,
        local_backup_path: StrPath,
        remote_path: StrPath,
    ):
        super().__init__()
        self.hostname = get_hostname(hostname).lower()
        self.username = username
        self.password = password
        self.remote_path = remote_path
        self.local_backup_path = local_backup_path
        self.local_backup_file_path = None  # to keep track of the actual file path
        self.remote_file_path = None  # to keep track of the actual file path
        self.entered: bool = False
        self.logger.debug(f"Initializing {type(self).__name__}")

    def __enter__(self):
        if self.entered:
            raise RuntimeError("already entered, cannot re-enter")
        self.entered = True
        self.connect()
        return self

    def connect(self):
        if (hasattr(self, "sftp") and not self.connected) or (
            not hasattr(self, "sftp")
        ):
            logger.debug("Acquiring SSH and SFTP Server connections")
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh.connect(
                self.hostname, username=self.username, password=self.password
            )
            self.sftp = self.ssh.open_sftp()

    def close(self):
        if self.connected:
            logger.debug("Closing SSH and SFTP Server connections")
            self.sftp.close()
            self.ssh.close()

    @property
    def connected(self) -> bool:
        return hasattr(self, "sftp") and not self.sftp.sock.closed

    @property
    def closed(self) -> bool:
        return not self.connected

    def _remote_path_exists(self, remote_path) -> bool:
        if not self.closed:
            try:
                self.sftp.stat(remote_path)
                return True
            except FileNotFoundError:
                return False
        raise RuntimeError("sftp server connection is closed")

    def _ensure_remote_path(self, remote_path):
        if not self._remote_path_exists(remote_path):
            self.sftp.mkdir(remote_path)

    def write(self, data):
        if self.connected:
            # Generate a unique filename using uuid4
            unique_filename = f'file_{uuid.uuid4().hex}.csv'
            if not os.path.exists(self.local_backup_path):
                os.makedirs(self.local_backup_path, exist_ok=True)
            self.local_backup_file_path = f'{self.local_backup_path}/{unique_filename}'
            logger.debug(
                f"Set local backup file path to '{self.local_backup_file_path}'"
            )

            with open(self.local_backup_file_path, 'w') as f:
                f.write('\n'.join(data))

            logger.debug(
                f"Wrote data to local backup file '{self.local_backup_file_path}'"
            )
            self._ensure_remote_path(self.remote_path)
            self.remote_file_path = f'{self.remote_path}/{unique_filename}'
            logger.debug(
                f"Copying local backup file '{self.local_backup_file_path}' "
                f"to remote file path '{self.remote_file_path}'"
            )
            self.sftp.put(self.local_backup_file_path, self.remote_file_path)
            return self.remote_file_path
        else:
            raise RuntimeError("sftp server connection is closed")

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        try:
            # If there's no exception and the local_backup_file_path exists, delete it
            if (
                exc_type is None
                and self.local_backup_file_path is not None
                and os.path.exists(self.local_backup_file_path)
            ):
                logger.debug(
                    "Successfully completed bulk insert operation; removing "
                    f"local backup file '{self.local_backup_file_path}'"
                )
                os.remove(self.local_backup_file_path)
            elif exc_type is not None:
                logger.error(
                    "Failed bulk insert operation; keeping local backup file "
                    f"'{self.local_backup_file_path}' for debug purposes"
                )
        finally:
            # Remove the remote_file_path
            if self.remote_file_path:
                logger.debug(f"Removing remote file '{self.remote_file_path}'")
                self.sftp.remove(self.remote_file_path)

            # Reset local_backup_file_path and remote_file_path
            logger.debug("Resetting local backup and remote file paths")
            self.local_backup_file_path = None
            self.remote_file_path = None

            # Close the SSH and SFTP server connections
            self.close()

            # Set enerted to False
            self.entered = False

    def __repr__(self):
        if self.connected:
            status = f"Connected {self.username}:****@{self.hostname}"
        else:
            status = "Closed"
        return f"<{self.__class__.__name__} {status}>"


class LocalTempHandler(AbstractTempHandler):

    def __init__(self, local_backup_path: str):
        super().__init__()
        self.local_backup_path = local_backup_path
        self.local_backup_file_path = None
        self.entered: bool = False
        logger.debug(f"Initializing {type(self).__name__}")

    def __enter__(self):
        if self.entered:
            raise RuntimeError("already entered, cannot re-enter")
        self.entered = True
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        try:
            # If there's no exception and the local_backup_file_path exists, delete it
            if (
                exc_type is None
                and self.local_backup_file_path is not None
                and os.path.exists(self.local_backup_file_path)
            ):
                logger.debug(
                    "Successfully completed bulk insert operation; removing "
                    f"local backup file '{self.local_backup_file_path}'"
                )
                os.remove(self.local_backup_file_path)

            elif exc_type is not None:
                logger.error(
                    "Failed bulk insert operation; keeping local backup file "
                    f"'{self.local_backup_file_path}' for debug purposes"
                )
        finally:
            # Reset local_backup_file_path
            logger.debug("Resetting local backup file path")
            self.local_backup_file_path = None
            self.entered = False

    def write(self, data: list[str]):
        # Generate a unique filename using uuid4
        unique_filename = f'file_{uuid.uuid4().hex}.csv'
        self.local_backup_file_path = f'{self.local_backup_path}/{unique_filename}'
        logger.debug(f"Set local backup file path to '{self.local_backup_file_path}'")

        with open(self.local_backup_file_path, 'w') as f:
            f.write('\n'.join(data))

        logger.debug(f"Wrote data to local backup file '{self.local_backup_file_path}'")
        return self.local_backup_file_path


def TempHandler(
    connstring: ConnString,
    local_backup_path: StrPath,
    remote_path: Optional[StrPath] = None,
) -> AbstractTempHandler:

    if connstring.islocal:
        return LocalTempHandler(str(local_backup_path))
    elif remote_path is None:
        raise ValueError("remote_path must be provided if connection is remote")
    else:
        from options_chain_pipeline.lib.env import SSH_SERVER
        from options_chain_pipeline.lib.env import SSH_USER
        from options_chain_pipeline.lib.env import SSH_PASSWORD

        assert isinstance(SSH_SERVER, str)
        assert isinstance(SSH_USER, str)
        assert isinstance(SSH_PASSWORD, str)

        return RemoteTempHandler(
            hostname=SSH_SERVER,
            username=SSH_USER,
            password=SSH_PASSWORD,
            local_backup_path=local_backup_path,
            remote_path=remote_path,
        )


class MemoryEfficientBuffer:
    def __init__(self, maxsize: int, temphandler: AbstractTempHandler):
        self.maxsize = maxsize
        self.temphandler = temphandler
        self.buffer = []

    def add(self, csv_strings):
        for csv_string in csv_strings:
            compressed_data = self.compress(csv_string)
            # Check if adding the new data would exceed maxsize
            if len(self.buffer) + 1 > self.maxsize:
                self.flush()
            self.buffer.append(compressed_data)

    def compress(self, csv_string):
        encoded_data = csv_string.encode('utf-8')
        return zlib.compress(encoded_data)

    def decompress(self, compressed_string):
        decompressed_data = zlib.decompress(compressed_string)
        return decompressed_data.decode('utf-8')

    def flush(self):
        if not self.buffer:
            return

        decompressed_data = [self.decompress(item) for item in self.buffer]

        with self.temphandler as handler:
            handler.write(decompressed_data)

        self.buffer = []


def main():
    # Example usage
    temphandler = RemoteTempHandler(
        'hostname',
        'username',
        'password',
        '/path/to/local/backup',
        '/path/to/remote',
    )
    buffer = MemoryEfficientBuffer(maxsize=10, temphandler=temphandler)

    # Adding a single row
    buffer.add(['col1,col2,col3'])

    # Adding multiple rows
    buffer.add(
        ['val1,val2,val3', 'val4,val5,val6', 'val7,val8,val9', 'val10,val11,val12']
    )

    # Adding enough rows to trigger a flush automatically
    buffer.add(
        [
            'row1_col1,row1_col2,row1_col3',
            'row2_col1,row2_col2,row2_col3',
            'row3_col1,row3_col2,row3_col3',
            'row4_col1,row4_col2,row4_col3',
            'row5_col1,row5_col2,row5_col3',
            'row6_col1,row6_col2,row6_col3',
        ]
    )

    # Additional rows can be added without manually flushing
    buffer.add(['row7_col1,row7_col2,row7_col3', 'row8_col1,row8_col2,row8_col3'])


if __name__ == "__main__":
    main()
