#!/usr/bin/env python3

import copy
import os
from pathlib import Path
from typing import Optional

from dotenv import find_dotenv
from dotenv import load_dotenv
from redis import Redis

from options_chain_pipeline.lib import PROJECT_ROOT
from options_chain_pipeline.lib.types import StrPath


class EnvironmentManager:

    DOTENV_PATH = os.path.dirname(__file__)
    # FILESYSTEM_PATH = os.path.join(DOTENV_PATH, "filesystem")
    REDIS_HASHNAME = "env"

    def __init__(self):
        self._existing = {"EXISTING": copy.deepcopy(self.get_fromenviron())}
        self._redis = {"REDIS": {}}
        self._dotenv = {"DOTENV": {}}
        # self._filesys = {"FILESYSTEM": {}}

    @property
    def partitioned_env(self):
        self.get_fromredis()
        self.get_fromdotenv()
        # self.get_fromfilesys()
        env = self._existing
        env.update(self._redis)
        env.update(self._dotenv)
        # env.update(self._filesys)
        return env

    def get_fromredis(self, hashname=None, to_environ: bool = False):

        hashname = hashname or self.REDIS_HASHNAME

        if env := Redis().hgetall(hashname):
            env_decoded = {
                k.decode().replace('"', ''): v.decode().replace('"', '')
                for k, v in env.items()
            }
            self._redis["REDIS"].update(env_decoded)
            if to_environ:
                os.environ.update(env_decoded)

        return self._redis

    def get_fromdotenv(self, dotenv_path: Optional[StrPath] = None):
        # sourcery skip: use-named-expression

        if dotenv_path is None:
            dotenv_path = self.DOTENV_PATH

        if not isinstance(dotenv_path, Path):
            dotenv_path = Path(dotenv_path).absolute()
        if not dotenv_path.exists():
            dotenv_path.mkdir()
        dotenv_path = dotenv_path / ".env"

        if load_dotenv(find_dotenv(str(dotenv_path), raise_error_if_not_found=True)):
            # if keys_loaded:

            env_after_loading_dotenv = self.get_fromenviron()

            if diff := {
                k: env_after_loading_dotenv[k]
                for k in env_after_loading_dotenv.keys()
                if k not in self._existing.keys()
            }:
                self._dotenv["DOTENV"].update(diff)

        return self._dotenv

    # def get_fromfilesys(
    #     self, filesys_path: Optional[StrPath] = None, to_environ: bool = True
    # ):

    #     filesys_path = filesys_path or self.FILESYSTEM_PATH

    #     if not isinstance(filesys_path, Path):
    #         filesys_path = Path(filesys_path)

    #     if not filesys_path.exists():
    #         filesys_path.mkdir()

    #     if keys := list(filesys_path.iterdir()):
    #         listdir = [filesys_path / key for key in keys]
    #         for filepath, key in zip(listdir, keys):
    #             with open(filepath, "r") as f:
    #                 value = f.read().strip()
    #             self._filesys["FILESYSTEM"][key] = value
    #             if to_environ:
    #                 os.environ[str(key)] = value

    #     return self._filesys

    def get_fromenviron(self):
        return dict(os.environ)

    def write_temporarily(self, **env_vars):
        if not env_vars:
            raise RuntimeError("No environment variables were passed to write")
        os.environ.update({k.upper(): str(v) for k, v in env_vars.items()})

