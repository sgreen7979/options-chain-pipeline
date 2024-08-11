#!/usr/bin/env python3

import json
import os
from pathlib import Path
import platform
import subprocess
from typing import Callable
from typing import Optional
from typing import overload

from redis import Redis

from options_chain_pipeline.lib.types import StrPath

from . import exc
from ._os import IS_LINUX
from ._os import IS_MAC
from ._os import IS_UNIX
from ._os import IS_WINDOWS


class Level:
    SYSTEM = 60
    USER = 50
    DOTENV = 40
    FILESYSTEM = 30
    REDIS = 20
    TEMPORARILY = 10
    NOTSET = 0

    @classmethod
    def add_level(cls, levelname, levelno):
        if cls._check_level_exists(levelno):
            raise exc.NewLevelException(
                f"levelno {levelno} already exists. Choose a new number."
            )
        if cls._check_levelname_exists(levelname):
            raise exc.NewLevelException(
                f"levelname '{levelname}' already exists. Choose a new name."
            )
        setattr(cls, levelname, levelno)

    @classmethod
    def _check_level_exists(cls, levelno):
        return levelno in cls.__dict__.values()

    @classmethod
    def _check_levelname_exists(cls, levelname):
        return levelname in cls.__dict__.keys()


_levelToName = {
    Level.SYSTEM: 'SYSTEM',
    Level.USER: 'USER',
    Level.DOTENV: 'DOTENV',
    Level.FILESYSTEM: 'FILESYSTEM',
    Level.REDIS: 'REDIS',
    Level.TEMPORARILY: 'TEMPORARILY',
    Level.NOTSET: 'NOTSET',
}

_nameToLevel = {
    'SYSTEM': Level.SYSTEM,
    'USER': Level.USER,
    'DOTENV': Level.DOTENV,
    'FILESYSTEM': Level.FILESYSTEM,
    'REDIS': Level.REDIS,
    'TEMPORARILY': Level.TEMPORARILY,
    'NOTSET': Level.NOTSET,
}


@overload
def getLevelName(level: int) -> str: ...


@overload
def getLevelName(level: str) -> int: ...


def getLevelName(level):
    """
    Return the textual or numeric representation of write level 'level'.

    If the level is one of the predefined levels (Level.SYSTEM, Level.USER,
    Level.DOTENV, Level.FILESYSTEM, Level.REDIS, Level.TEMPORARILY,
    Level.NOTSET) then you get the corresponding string. If you have
    associated levels with names using addLevelName then the name you have
    associated with 'level' is returned.

    If a numeric value corresponding to one of the defined levels is passed
    in, the corresponding string representation is returned.

    If a string representation of the level is passed in, the corresponding
    numeric value is returned.

    If no matching numeric or string value is passed in, a ValueError is
    raised.
    """
    result = _levelToName.get(level)
    if result is not None:
        return result
    result = _nameToLevel.get(level)
    if result is not None:
        return result
    else:
        raise ValueError(f"level passed ({level}) is invalid")


def addLevelName(level, levelName):
    """
    Associate 'levelName' with 'level'.

    This is used when converting levels to text during message formatting.
    """
    try:  # unlikely to cause an exception, but you never know...
        Level.add_level(levelName, level)
        _levelToName[level] = levelName
        _nameToLevel[levelName] = level
    except exc.NewLevelException:
        raise


class EnvironmentWriter:
    """
    Environment variable writer intended to work on
    both Windows- as Unix-based operating systems.

        Example usages::

            ```python
            from options_chain_pipeline.lib.env.writer import EnvironmentWriter

            # write a user environment variable
            # NOTE: usually need to restart the machine for this to take effect
            writer = EnvironmentWriter(EnvironmentWriter.Level.USER)
            writer.write(MY_ENV_VAR="my_value")

            # write a system environment variable
            # NOTE: usually need to restart the machine for this to take effect
            writer = EnvironmentWriter(EnvironmentWriter.Level.SYSTEM)
            writer.write(MY_ENV_VAR="my_value")

            # set an environment variable temporarily (i.e., in the context
            # of the current process only)
            writer = EnvironmentWriter()
            writer.write(MY_ENV_VAR="my_value")
            ```

    """

    def __init__(
        self,
        level: int = Level.TEMPORARILY,
        *,
        write_func: Optional[Callable] = None,
    ) -> None:
        if not Level._check_level_exists(level):
            valid_levels = ', '.join([str(k) for k in sorted(_levelToName.keys())])
            raise ValueError(
                f"You passed an invalid level '{level}'. "
                f"Valid levels: {valid_levels}"
            )

        self.levelname = getLevelName(level)
        self.levelno = level
        self._set_write_func(write_func)

    @property
    def levelno(self):
        return self._levelno

    @levelno.setter
    def levelno(self, level):
        if level in _nameToLevel:
            self._levelno = _nameToLevel[level]

        elif level in _levelToName:
            self._levelno = level

        else:
            raise ValueError(f"Invalid level passed ({level})")

    @property
    def write(self) -> Callable:
        """
        Example usages::

            ```python
            from options_chain_pipeline.lib.env.writer import EnvironmentWriter

            # write a user environment variable
            # NOTE: usually need to restart the machine for this to take effect
            writer = EnvironmentWriter(EnvironmentWriter.Level.USER)
            writer.write(MY_ENV_VAR="my_value")

            # write a system environment variable
            # NOTE: usually need to restart the machine for this to take effect
            writer = EnvironmentWriter(EnvironmentWriter.Level.SYSTEM)
            writer.write(MY_ENV_VAR="my_value")

            # set an environment variable temporarily (i.e., in the context
            # of the current process only)
            writer = EnvironmentWriter()
            writer.write(MY_ENV_VAR="my_value")
            ```
        """
        return self._write

    setenv = write

    @write.setter
    def write(self, func: Callable):
        if not callable(func):
            raise TypeError(
                "the write property must be set to a Callable, "
                f"got {func.__class__.__name__}"
            )
        self._write = func

    def _get_write_func(self):
        return getattr(self, self.levelname, lambda: NotImplemented)

    def _set_write_func(self, func=None):
        self.write = func or self._get_write_func()

    def system(self, **env_vars):
        if IS_UNIX:
            env_vars = json.dumps(env_vars)
            cmd = f"sudo python3 -m options_chain_pipeline.lib.env._admin {env_vars} --sys"
            proc = subprocess.Popen(
                args=cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            proc.wait()
            if proc.stderr:
                # stderr = proc.stderr.decode("utf-8")
                stderr = proc.stderr.read().decode("utf-8")
                print("STDERR:\n" + stderr)

        elif IS_WINDOWS:

            import winreg

            # Open the registry key for the system environment variables
            subkey = "SYSTEM\\CurrentControlSet\\Control\\Session Manager\\Environment"
            system_env_key = winreg.OpenKey(
                winreg.HKEY_LOCAL_MACHINE, subkey, 0, winreg.KEY_ALL_ACCESS
            )

            # Set the environment variables
            for key, value in env_vars.items():
                winreg.SetValueEx(system_env_key, key, 0, winreg.REG_EXPAND_SZ, value)

            # Close the registry key
            winreg.CloseKey(system_env_key)

    SYSTEM = system

    def user(self, **env_vars):
        if IS_UNIX:
            env_vars = json.dumps(env_vars)
            cmd = f"sudo python3 -m options_chain_pipeline.lib.env._admin {env_vars} --usr"
            proc = subprocess.Popen(
                args=cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            stdout, stderr = proc.communicate()

            if stderr:
                stderr = stderr.decode("utf-8")
                print("STDERR:\n" + stderr)

            if stdout:
                stdout = stdout.decode("utf-8")
                print("STDOUT:\n" + stdout)

        elif IS_WINDOWS:

            import winreg

            # Open the registry key for the user's environment variables
            user_env_key = winreg.OpenKey(
                winreg.HKEY_CURRENT_USER, "Environment", 0, winreg.KEY_ALL_ACCESS
            )

            # Set the environment variables
            for key, value in env_vars.items():
                winreg.SetValueEx(user_env_key, key, 0, winreg.REG_EXPAND_SZ, value)

            # Close the registry key
            winreg.CloseKey(user_env_key)

        else:
            return NotImplemented

    USER = user

    def temporarily(self, **env_vars):
        for k, v in env_vars.items():
            os.environ[k.upper()] = str(v)

    TEMPORARILY = temporarily

    def dotenv(self, dotenv_path: Optional[StrPath] = None, **env_vars):
        if dotenv_path is None or not os.path.exists(dotenv_path):
            dotenv_path = Path("./.env")
        else:
            dotenv_path = Path(dotenv_path)

            if dotenv_path.is_dir():
                dotenv_path = dotenv_path / ".env"

        if not dotenv_path.exists():
            with open(dotenv_path, "w") as f:
                lines = []
                for k, v in env_vars.items():
                    lines.append(f"{k}={v}")
                    # line = f"\n{k}={v}"
                    # f.write(line)
                    f.write("\n".join(lines))

        else:
            with open(dotenv_path, "r") as f:
                oldlines = f.readlines()

            with open(dotenv_path, "w") as f:
                for k, v in env_vars.items():
                    for oldline in oldlines:
                        if not oldline.strip():
                            newline = '\n'
                        elif f"{k}=" in oldline:
                            inline_comment = ""
                            if "#" in oldline:
                                inline_comment = "  # " + oldline.split("#")[-1].strip()
                            newline = f"\n{k}={v}{inline_comment}"
                        else:
                            newline = f"\n{oldline}"
                        f.write(newline)

    DOTENV = dotenv

    def filesystem(self, path: Optional[StrPath] = None, **env_vars):
        if path is None or not os.path.exists(path):
            path = Path("./")
        else:
            path = Path(path)
            if not path.is_dir():
                raise ValueError(f"path is not a directory {path}")

        for k, v in env_vars.items():
            file_path = path / k
            with open(file_path, "w") as f:
                f.write(v)

    FILESYSTEM = filesystem

    def redis(self, hashname="env", **env_vars):
        for key, val in env_vars.items():
            Redis().hset(hashname, key, val)

    REDIS = redis

    def notset(self, **env_vars):
        return NotImplemented

    NOTSET = notset

    @classmethod
    def with_newlevel(cls, newlevel: int, newlevel_name: str, newlevel_write: Callable):
        if not callable(newlevel_write):
            raise exc.ArgumentError(
                "New write function passed must be callable. "
                f"{newlevel_write.__class__.__name__} "
                "is not callable."
            )
        addLevelName(newlevel, newlevel_name)
        inst = cls(level=newlevel)
        setattr(inst, newlevel_name, newlevel_write)
        return inst

    Level = Level
