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

    # @staticmethod
    # def grant_registry_permissions(key, user):
    #     # `reg_key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, key, 0, winreg.KEY_ALL_ACCESS)` is
    #     # opening a registry key in the Windows registry. The `winreg.HKEY_LOCAL_MACHINE` argument
    #     # specifies that the key is located in the `HKEY_LOCAL_MACHINE` section of the registry. The
    #     # `key` argument specifies the name of the key to open. The `0` argument specifies that the
    #     # key should be opened with the default options. The `winreg.KEY_ALL_ACCESS` argument
    #     # specifies that the key should be opened with full access rights, allowing the program to
    #     # read, write, and delete values in the key. The resulting `reg_key` variable is a handle to
    #     # the opened key, which can be used to read or modify its values.
    #     reg_key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, key, 0, winreg.KEY_ALL_ACCESS)
    #     # `sd = winreg.QuerySecurityAccess(reg_key, winreg.DACL_SECURITY_INFORMATION)` is querying the
    #     # security descriptor of the Windows registry key `reg_key` to retrieve the discretionary
    #     # access control list (DACL) information. The `winreg.DACL_SECURITY_INFORMATION` argument
    #     # specifies that only the DACL information should be retrieved. The resulting `sd` variable
    #     # contains the security descriptor information, which can be used to modify the access control
    #     # list for the registry key.
    #     sd = winreg.QuerySecurityAccess(reg_key, winreg.DACL_SECURITY_INFORMATION)
    #     # `dacl = sd.GetSecurityDescriptorDacl()` is retrieving the discretionary access control list
    #     # (DACL) from the security descriptor of the registry key. The DACL is a list of access
    #     # control entries (ACEs) that specify which users or groups are allowed or denied access to
    #     # the key and what level of access they have.
    #     dacl = sd.GetSecurityDescriptorDacl()
    #     # `sid = winreg.LookupAccountName(None, user)[0]` is looking up the security identifier (SID)
    #     # for the specified user. The `LookupAccountName` function takes two arguments: the name of
    #     # the system to look up the account on (in this case, `None` means the local system), and the
    #     # name of the user to look up. It returns a tuple containing the SID and the name of the
    #     # domain the user belongs to (if any). The `[0]` at the end of the line is indexing the tuple
    #     # to get just the SID.
    #     sid = winreg.LookupAccountName(None, user)[0]
    #     # `dacl.AddAccessAllowedAce(winreg.ACL_REVISION_DS, winreg.ACCESS_MASK(winreg.KEY_ALL_ACCESS),
    #     # sid)` is adding an access control entry (ACE) to the discretionary access control list
    #     # (DACL) of a Windows registry key. Specifically, it is adding an ACE that allows the
    #     # specified user (identified by their security identifier, or SID) to have full control over
    #     # the registry key (i.e. `winreg.KEY_ALL_ACCESS`). The `winreg.ACL_REVISION_DS` argument
    #     # specifies the revision level of the access control list.
    #     dacl.AddAccessAllowedAce(winreg.ACL_REVISION_DS, winreg.ACCESS_MASK(winreg.KEY_ALL_ACCESS), sid)
    #     # `winreg.SetNamedSecurityInfo(key, winreg.SE_REGISTRY_KEY, winreg.DACL_SECURITY_INFORMATION,
    #     # None, None, dacl, None)` is a method call that sets the security information for a specified
    #     # registry key. The `key` argument specifies the registry key to set the security information
    #     # for, and `winreg.SE_REGISTRY_KEY` specifies that the key is a registry key.
    #     # `winreg.DACL_SECURITY_INFORMATION` specifies that the discretionary access control list
    #     # (DACL) is being set. The `None` arguments are placeholders for other security information
    #     # that can be set (such as the owner or group), but are not being set in this case. `dacl` is
    #     # the new DACL to set for the key, and `None` is passed as the last argument to indicate that
    #     # no audit information is being set.
    #     winreg.SetNamedSecurityInfo(key, winreg.SE_REGISTRY_KEY, winreg.DACL_SECURITY_INFORMATION,
    #                                 None, None, dacl, None)
