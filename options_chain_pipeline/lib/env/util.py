#!/usr/bin/env python3

import os
from pathlib import Path
import subprocess


def get_module():
    relpath = os.path.relpath(__name__)
    return relpath.split(".")[0] if "." in relpath else relpath


def get_user():
    from ._os import IS_UNIX
    from ._os import IS_WINDOWS

    if IS_UNIX:
        import pwd

        uid = os.getuid()  # type: ignore
        return pwd.getpwuid(uid).pw_name  # type: ignore
    elif IS_WINDOWS:
        return os.getlogin()
    else:
        return NotImplemented


def set_user_as_admin(username=get_user()):
    from ._os import IS_UNIX
    from ._os import IS_WINDOWS

    if is_admin(username):
        return

    if IS_UNIX:
        cmd = f"sudo dseditgroup -o edit -a {username} -t user admin"
    elif IS_WINDOWS:
        cmd = f"net localgroup administrators {username} /add"
    else:
        # raise OSError("Unrecognized OS ('{OS}')")
        return NotImplemented

    make_syscall(cmd=cmd, raise_on_failure=True)


def get_administrators():
    """
    Get a list all user accounts within the Administrators group
    """
    from ._os import IS_UNIX
    from ._os import IS_WINDOWS

    if IS_UNIX:
        # Run the dscl command to get the list of members of the "admin" group
        output = subprocess.check_output(
            ["dscl", ".", "-read", "/Groups/admin", "GroupMembership"]
        )

        # Extract the list of members from the output
        members = output.decode("utf-8").strip().split(":")[1].split()

    elif IS_WINDOWS:
        # Run the net localgroup command to get the list of members
        # of the "Administrators" group
        output = subprocess.check_output(["net", "localgroup", "Administrators"])

        # Extract the list of members from the output
        members = output.decode("utf-8").strip().split("\r\n")[:-1][6:]

    else:
        return NotImplemented

    return members


def is_admin(username=get_user()):
    return username in get_administrators()


def make_syscall(cmd: str | list[str], raise_on_failure: bool = False) -> bool:
    if isinstance(cmd, list):
        cmd = " ".join(cmd)
    returncode = os.system(cmd)
    if returncode != 0:
        if raise_on_failure:
            raise OSError(f"System call failed (command='{cmd}')")
        return False
    return True


def get_dotenv_path(path):
    return Path(os.path.relpath(path)).parent / ".env"
