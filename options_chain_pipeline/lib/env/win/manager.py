#!/usr/bin/env python3

import os
import winreg

from ..manager import EnvironmentManager


class WinEnvironmentManager(EnvironmentManager):

    SYSTEM_SUBKEY = "SYSTEM\\CurrentControlSet\\Control\\Session Manager\\Environment"
    USER_SUBKEY = "Environment"
    USER = os.getlogin()

    def write_to_user(
        self,
        **env_vars
    ):
        with winreg.OpenKey(
            key=winreg.HKEY_CURRENT_USER,
            sub_key=self.USER_SUBKEY,
            reserved=0,
            access=winreg.KEY_ALL_ACCESS
        ) as user_env_key:

            self._set_values(user_env_key, **env_vars)

    def write_to_system(
        self,
        **env_vars
    ):
        with winreg.OpenKey(
            key=winreg.HKEY_LOCAL_MACHINE,
            sub_key=self.SYSTEM_SUBKEY,
            reserved=0,
            access=winreg.KEY_ALL_ACCESS
        ) as system_env_key:

            self._set_values(system_env_key, **env_vars)

    def _set_values(self, envkey, **env_vars):
        """Set the environment variables"""
        for key, value in env_vars.items():
            winreg.SetValueEx(
                envkey, key, 0, winreg.REG_EXPAND_SZ, value)
