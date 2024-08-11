#!/usr/bin/env python3

from .._os import IS_WINDOWS

if IS_WINDOWS:
    import winreg
    import win32api
    import win32security


    def grant_registry_permissions(key, user):
        """Grant full control permission to a user on a registry key

        Args:
            key (_type_): _description_
            user (_type_): _description_

        # example usage
        >>> grant_registry_permissions(r"SOFTWARE\Microsoft\Windows NT\CurrentVersion", "myusername")
        """
        reg_key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, key, 0, winreg.KEY_ALL_ACCESS)
        key_handle = win32security.OpenProcessToken(
            win32api.GetCurrentProcess(),
            win32security.TOKEN_ADJUST_PRIVILEGES | win32security.TOKEN_QUERY)
        privilege_id = win32security.LookupPrivilegeValue(None, win32security.SE_RESTORE_NAME)
        token_privileges = [(privilege_id, win32security.SE_PRIVILEGE_ENABLED)]
        win32security.AdjustTokenPrivileges(key_handle, False, token_privileges)
        sd = win32security.GetKernelObjectSecurity(reg_key, win32security.DACL_SECURITY_INFORMATION)
        dacl = sd.GetSecurityDescriptorDacl()
        sid = win32security.LookupAccountName(None, user)[0]
        dacl.AddAccessAllowedAce(win32security.ACL_REVISION_DS, winreg.KEY_ALL_ACCESS, sid)
        win32security.SetNamedSecurityInfo(key, win32security.SE_REGISTRY_KEY, win32security.DACL_SECURITY_INFORMATION, None, None, dacl, None)

else:
    def grant_registry_permissions(key, user):
        raise OSError("`grant_registry_permissions` is compatible with Windows machines only")