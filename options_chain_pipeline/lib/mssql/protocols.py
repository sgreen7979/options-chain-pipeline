#!/usr/bin/env python3
from typing import Protocol
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daily.sql.mssql.connection_string import ConnString


class HasConfiguredConnectionString(Protocol):
    """Has a class attribute named ConfiguredConnectionString

    Examples:
        ```python
        class MyMSSQLConfig:
            # Pass parameters directly
            ConfiguredConnectionString: ClassVar[ConnString] = ConnString(
                server = "192.168.1.198",
                database = "db_name",
                uid = "user",
                pwd = "some_password",
                driver = "ODBC Driver 17 for SQL Server",
            )


        class MyLocalMSSQLConfig:
            # `ConnString()` with no params passed returns a local connection string
            # to the 'master' database
            ConfiguredConnectionString: ClassVar[ConnString] = ConnString()


        class MyMSSQLConfig:
            # Pass parameters from environment variables
            ConfiguredConnectionString: ClassVar[ConnString] = ConnString(
                server = os.getenv("PRIMARY_MSSQL_SERVER", None),
                database = "db_name",
                uid = os.getenv("MSSQL_UID", None),
                pwd = os.getenv("MSSQL_PWD", None),
                driver = os.getenv("MSSQL_DRIVER", None),
            )
        ```
    """

    ConfiguredConnectionString: "ConnString"
