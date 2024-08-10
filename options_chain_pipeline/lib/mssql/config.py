#!/usr/bin/env python3
import os
from typing import cast
from typing import ClassVar
from typing import Dict
from typing import Optional
from typing import Tuple

from daily.sql.mssql.connection_string import ConnString
from daily.utils.networking import is_local
from daily.utils.networking import get_local_ipv4
from daily.utils.networking import get_local_hostname


class MSSQLConfig:

    LocalConnectionString: ClassVar[ConnString] = ConnString()
    LocalHostname: ClassVar[str] = get_local_hostname()
    LocalIPv4: ClassVar[str] = get_local_ipv4()
    LocalIpAndHost: ClassVar[Tuple[str, str]] = (LocalIPv4, LocalHostname)
    LocalServer: ClassVar[str] = LocalConnectionString.server
    LocalServerUID: ClassVar[str] = cast(str, LocalConnectionString.uid)

    PrimarySQLServer: ClassVar[str] = os.getenv("SQL_PRIMARY_SERVER", LocalServer)
    PrimarySQLServerIsLocal: ClassVar[bool] = is_local(PrimarySQLServer)
    PrimarySQLServerIsRemote: ClassVar[bool] = not PrimarySQLServerIsLocal

    SQLServerUID: ClassVar[Optional[str]] = os.getenv("SQL_UID", None)
    SQLServerPWD: ClassVar[Optional[str]] = os.getenv("SQL_PWD", None)
    SQLServerPort: ClassVar[str] = os.getenv("SQL_PORT", "1433")

    ConfiguredConnectionString: ClassVar[ConnString] = ConnString(
        server=PrimarySQLServer, uid=SQLServerUID, pwd=SQLServerPWD
    )

    JdbcConnectionProperties: ClassVar[Dict[str, str]] = {
        "user": SQLServerUID or "NONE",
        "password": SQLServerPWD or "NONE",
        # "trustedConnection": str(ConfiguredConnectionString.trusted_conn).lower(),
        "trustServerCertificate": str(
            ConfiguredConnectionString.trust_server_cert
        ).lower(),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        # "batchsize": "500",
        # "isolationLevel": "READ_COMMITTED",  # Ensure consistent read/write operations
        # "isolationLevel": "NONE",  # Ensure consistent read/write operations
        # "rewriteBatchedStatements": "true",
    }

    isTested: ClassVar[bool] = False
    isSafe: ClassVar[bool] = False

    @classmethod
    def setConfiguredConnectionString(cls, connstr: ConnString):
        if connstr != cls.ConfiguredConnectionString:
            cls.ConfiguredConnectionString = connstr
            cls.PrimarySQLServer = connstr.server
            cls.SQLServerUID = connstr.uid
            cls.SQLServerPWD = connstr.pwd
            cls.PrimarySQLServerIsLocal = is_local(cls.PrimarySQLServer)
            cls.PrimarySQLServerIsRemote = not cls.PrimarySQLServerIsLocal
            cls.isTested = cls.isSafe = False
            cls.JdbcConnectionProperties.update(
                {
                    "user": cls.SQLServerUID or "NONE",
                    "password": cls.SQLServerPWD or "NONE",
                    "trustServerCertificate": str(
                        cls.ConfiguredConnectionString.trust_server_cert
                    ).lower(),
                }
            )

    @classmethod
    def testConfiguredConnectionString(cls):
        import pyodbc

        cls.isTested = True
        try:
            conn = pyodbc.connect(
                cls.ConfiguredConnectionString.value(), autocommit=True
            )
            assert not getattr(conn, "closed")
        except Exception as e:
            import warnings

            warnings.warn(
                "Failed to connect with ConfiguredConnectionString\n\t"
                f"{str(cls.ConfiguredConnectionString)}\n\n"
                f"{e.with_traceback(e.__traceback__)}"
            )
        else:
            cls.isSafe = True
            conn.close()

    @classmethod
    def get(cls, attr, default=None):
        return getattr(cls, attr, default)
