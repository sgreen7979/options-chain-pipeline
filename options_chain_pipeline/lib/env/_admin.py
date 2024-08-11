#!/usr/bin/env python3

import json

from ._os import IS_UNIX
from ._os import IS_WINDOWS
from ._os import OS


def get_manager_class():
    if IS_WINDOWS:
        from ._win import WinEnvironmentManager as manager_class
    elif IS_UNIX:
        # FIXME missing UnixEnvironmentManager
        from ._unix import UnixEnvironmentManager as manager_class
    else:
        raise OSError(f"No implementation for OS '{OS}'")

    return manager_class


def main():

    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("env_vars", type=json.loads)
    parser.add_argument(
        "--usr",
        default=False,
        action="store_true",
        help="Write environment variables to user",
    )
    parser.add_argument(
        "--sys",
        default=False,
        action="store_true",
        help="Write environment variables to system",
    )
    args = parser.parse_args()

    manager_class = get_manager_class()
    manager = manager_class()

    if args.sys:
        manager.write_to_system(**args.env_vars)
    elif args.usr:
        manager.write_to_user(**args.env_vars)
    else:
        manager.write_temporarily(**args.env_vars)


if __name__ == "__main__":
    main()
