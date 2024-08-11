#!/usr/bin/env python3
import datetime as dt
import logging
import os
from typing import List

from options_chain_pipeline.lib import DATA_PATH
from options_chain_pipeline.lib import LOG_PATH
from options_chain_pipeline.lib import MODULE
from options_chain_pipeline.lib import PROJECT_ROOT
from options_chain_pipeline.lib.types import StrPath
from options_chain_pipeline.lib.utils.logging import get_logger
from options_chain_pipeline.lib.utils import networking as net

from ._os import OS as OS  # noqa: F401
from ._os import IS_LINUX as IS_LINUX  # noqa: F401
from ._os import IS_MAC as IS_MAC  # noqa: F401
from ._os import IS_UNIX as IS_UNIX  # noqa: F401
from ._os import IS_WINDOWS as IS_WINDOWS  # noqa: F401
from .writer import EnvironmentWriter

logger = get_logger(__name__, ch=True, fh=True)

__all__ = [
    "DATA_PATH",
    "EnvironmentWriter",
    "LOCAL_HOSTNAME",
    "LOCAL_IPv4",
    "LOCAL_UID",
    "LOG_PATH",
    "MODULE",
    "PROJECT_ROOT",
    "SQL_PRIMARY_SERVER",
    "SQL_UID",
    "SQL_PWD",
    "SSH_SERVER",
    "SSH_USER",
    "SSH_PASSWORD",
    "USER_AGENT",
]


USER_AGENT_ENV_VARNAME = "USER_AGENT"
SQL_PRIMARY_SERVER_ENV_VARNAME = "SQL_PRIMARY_SERVER"
SQL_UID_ENV_VARNAME = "SQL_UID"
SQL_PWD_ENV_VARNAME = "SQL_PWD"
SSH_SERVER_ENV_VARNAME = SQL_PRIMARY_SERVER_ENV_VARNAME
SSH_USER_ENV_VARNAME = "SQL_PRIMARY_SERVER_USERNAME"
SSH_PASSWORD_ENV_VARNAME = "SQL_PRIMARY_SERVER_PASSWORD"

USER_AGENT = os.getenv(USER_AGENT_ENV_VARNAME)

LOCAL_HOSTNAME = net.get_local_hostname()
LOCAL_IPv4 = net.get_local_ipv4()
LOCAL_UID = net.get_local_uid(LOCAL_HOSTNAME)
SQL_PRIMARY_SERVER = os.getenv(SQL_PRIMARY_SERVER_ENV_VARNAME)
SQL_UID = os.getenv(SQL_UID_ENV_VARNAME)
SQL_PWD = os.getenv(SQL_PWD_ENV_VARNAME)
SSH_SERVER = os.getenv(SSH_SERVER_ENV_VARNAME)
SSH_USER = os.getenv(SSH_USER_ENV_VARNAME)
SSH_PASSWORD = os.getenv(SSH_PASSWORD_ENV_VARNAME)

none_values = []

for env_var_val, env_var_name, name in (
    (USER_AGENT, USER_AGENT_ENV_VARNAME, "USER_AGENT"),
    (SQL_PRIMARY_SERVER, SQL_PRIMARY_SERVER_ENV_VARNAME, "SQL_PRIMARY_SERVER"),
    (SQL_UID, SQL_UID_ENV_VARNAME, "SQL_UID"),
    (SQL_PWD, SQL_PWD_ENV_VARNAME, "SQL_PWD"),
    (SSH_SERVER, SSH_SERVER_ENV_VARNAME, "SSH_SERVER"),
    (SSH_USER, SSH_USER_ENV_VARNAME, "SSH_USER"),
    (SSH_PASSWORD, SSH_PASSWORD_ENV_VARNAME, "SSH_PASSWORD"),
):
    if env_var_val is None:
        logger.warning(f"'{env_var_name}' is not set, {ocp.MODULE}.env.{name} is None")
