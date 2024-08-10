#!/usr/bin/env python3
from contextlib import contextmanager
from typing import Generator
from typing import Optional

from daily.sql.mssql.client import MSSQLClient
from daily.sql.mssql.config import MSSQLConfig
from daily.sql.mssql.protocols import HasConfiguredConnectionString
from daily.utils.logging import get_logger

logger = get_logger("daily.sql.mssql.functions", level="DEBUG", ch=True)

GLOBAL_CONFIG: HasConfiguredConnectionString = MSSQLConfig
MAX_CAPACITY: Optional[int] = None


def set_global_config(config: HasConfiguredConnectionString):
    global GLOBAL_CONFIG
    GLOBAL_CONFIG = config


def get_number_active_connections() -> int:
    return MSSQLClient.get_num_connected()


def set_max_capacity(max_capacity: Optional[int] = None) -> None:
    global MAX_CAPACITY
    MAX_CAPACITY = max_capacity


def has_capacity() -> bool:
    global MAX_CAPACITY
    if MAX_CAPACITY is None:
        return True
    return get_number_active_connections() < MAX_CAPACITY


def get_current_capacity() -> Optional[int]:
    global MAX_CAPACITY
    if MAX_CAPACITY is not None:
        return MAX_CAPACITY - get_number_active_connections()


@contextmanager
def get_sql_connection(
    config: Optional[HasConfiguredConnectionString] = None, autocommit: bool = True
) -> Generator[MSSQLClient, None, None]:
    """Get a MSSQLClient instance that represents one connection

    :param config: config with ClassVar called "ConfiguredConnectionString", defaults to None
    :type config: Optional[HasConfiguredConnectionString], optional
    :param autocommit: autocommit executed sql queries, defaults to True
    :type autocommit: bool, optional
    :yield: the MSSQLCLient instance
    :rtype: Generator[MSSQLClient, None, None]

    Usage:
        ```python
        with get_sql_connection() as sql_client:
            sql_client.execute("<SQL QUERY>")
        ```
    """
    conn_str = (config or GLOBAL_CONFIG).ConfiguredConnectionString
    sql_client = MSSQLClient(conn_str, autocommit=autocommit)
    # logger.info(f"Created new SQL connection and client at {hex(id(sql_client))}")
    try:
        yield sql_client
    except Exception as e:
        sql_client.get_logger().error(e, exc_info=True)
        # raise
    finally:
        sql_client.close()
