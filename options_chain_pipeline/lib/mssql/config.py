#!/usr/bin/env python3
import os
from typing import cast
from typing import ClassVar
from typing import Dict
from typing import Optional
from typing import Tuple

from options_chain_pipeline.lib.mssql import ConnString
from options_chain_pipeline.lib.networking import is_local
from options_chain_pipeline.lib.networking import get_local_ipv4
from options_chain_pipeline.lib.utils.networking import get_local_hostname


class MSSQLConfig:

    LocalConnectionString: ClassVar[ConnString] = ConnString()
    LocalHostname: ClassVar[str] = get_local_hostname()
    LocalIPv4: ClassVar[str] = get_local_ipv4()
    LocalIpAndHost: ClassVar[Tuple[str, str]] = (LocalIPv4, LocalHostname)
    LocalServer: ClassVar[str] = LocalConnectionString.server
    LocalServerUID: ClassVar[str] = cast(str, LocalConnectionString.uid)

    PrimaryServer: ClassVar[str] = os.getenv("SQL_PRIMARY_SERVER", LocalServer)
    PrimaryServerIsLocal: ClassVar[bool] = is_local(PrimaryServer)
    PrimaryServerIsRemote: ClassVar[bool] = not PrimaryServerIsLocal

    UID: ClassVar[Optional[str]] = os.getenv("SQL_UID", None)
    PWD: ClassVar[Optional[str]] = os.getenv("SQL_PWD", None)
    Port: ClassVar[str] = os.getenv("SQL_PORT", "1433")

    ConnectionString: ClassVar[ConnString] = ConnString(
        server=PrimaryServer, uid=UID, pwd=PWD
    )

    JdbcConnectionProperties: ClassVar[Dict[str, str]] = {
        "user": UID or "NONE",
        "password": PWD or "NONE",
        "trustServerCertificate": str(ConnectionString.trust_server_cert).lower(),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }

    isTested: ClassVar[bool] = False
    isSafe: ClassVar[bool] = False

    @classmethod
    def setConfiguredConnectionString(cls, connstr: ConnString):
        if connstr != cls.ConfiguredConnectionString:
            cls.ConnectionString = connstr
            cls.PrimaryServer = connstr.server
            cls.UID = connstr.uid
            cls.PWD = connstr.pwd
            cls.PrimaryServerIsLocal = is_local(cls.PrimaryServer)
            cls.PrimaryServerIsRemote = not cls.PrimaryServerIsLocal
            cls.isTested = cls.isSafe = False
            cls.JdbcConnectionProperties.update(
                {
                    "user": cls.UID or "NONE",
                    "password": cls.PWD or "NONE",
                    "trustServerCertificate": str(connstr.trust_server_cert).lower(),
                }
            )

    @classmethod
    def testConfiguredConnectionString(cls):
        import pyodbc

        cls.isTested = True
        try:
            conn = pyodbc.connect(
                cls.ConnectionString.value(), autocommit=True
            )
            assert not getattr(conn, "closed")
        except Exception as e:
            import warnings

            warnings.warn(
                "Failed to connect with ConnectionString\n\t"
                f"{str(cls.ConnectionString)}\n\n"
                f"{e.with_traceback(e.__traceback__)}"
            )
        else:
            cls.isSafe = True
            conn.close()

    @classmethod
    def get(cls, attr, default=None):
        return getattr(cls, attr, default)
