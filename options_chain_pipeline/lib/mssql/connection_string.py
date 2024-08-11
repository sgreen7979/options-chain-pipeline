#!/usr/bin/env python3
import os
import platform
import pyodbc
import re
from types import SimpleNamespace
from typing import Dict
from typing import Optional

from options_chain_pipeline.lib.env import IS_LINUX
from options_chain_pipeline.lib.env import IS_WINDOWS
from options_chain_pipeline.lib.env import LOCAL_HOSTNAME
from options_chain_pipeline.lib.env import LOCAL_IPv4

from . import exceptions as exc


class ConnString:
    """Connection string object.

    Get the underlying connection string by calling
    `ConnString`'s :meth:`value` method. Or,
    equivalently, `str(cstring_inst)`.
    """

    def __init__(
        self,
        server: Optional[str] = None,
        database: Optional[str] = None,
        uid: Optional[str] = None,
        pwd: Optional[str] = None,
        trusted_conn: Optional[bool] = None,
        trust_server_cert: Optional[bool] = None,
        *,
        driver: Optional[str] = None,
    ) -> None:
        """Construct a connection string for SQL Server.

        If no non-None parameters are passed, a local
        server connection string to the 'master' database
        will be constructed.

        The default ODBC driver is ODBC Driver 17 for
        SQL Server.

        :param server: the server, defaults to local server
        :type server: Optional[str]

        :param database: the database, defaults to 'master'
        :type database: Optional[str]

        :param uid: the user id, defaults to local server//user;
                    required if the target server is remote
        :type uid: Optional[str]

        :param pwd: the user password, defaults to None;
                    required if the target server is remote
        :type pwd: Optional[str]

        :param trusted_conn: trusted local server connection,
                             defaults to True if server is local
        :type trusted_conn: Optional[bool]

        :param trust_server_cert: trust the remote server
                                  certificate (**risky**),
                                  defaults to False unless
                                  server is remote
        :type trust_server_cert: Optional[bool]

        :param driver: the pyodbc driver, defaults to ODBC
                       Driver 17 for SQL Server if None
        :type driver: Optional[str]

        :raises IncompleteParametersException: _description_
        """
        if not IS_WINDOWS and any(
            [
                server is None,
                uid is None,
                pwd is None,
            ]
        ):
            raise exc.IncompleteParametersException(
                "When using Mac, Linux, or any non-Windows OS, you "
                f"must provide parameters for `server` ('{server}'), "
                f"`uid` ('{uid}'), and `pwd` ('****') to the "
                f"{self.__class__.__name__} constructor"
            )

        self.server = server or LOCAL_HOSTNAME
        self.database = database or "master"
        self.uid = uid
        self.pwd = pwd
        if driver is not None:
            driver = self._normalize_driver(driver)
        self.driver = driver or "ODBC Driver 17 for SQL Server"
        self._assert_driver_installation(self.driver)
        self._set_security_params(trusted_conn, trust_server_cert)

    @staticmethod
    def _normalize_driver(driver: str) -> str:
        while driver.endswith("}"):
            driver = driver.removesuffix("}")
        while driver.startswith("{"):
            driver = driver.removeprefix("{")
        return driver

    @staticmethod
    def _assert_driver_installation(driver):
        if driver not in pyodbc.drivers():
            raise exc.MissingDriverError(driver)

    def value(self):
        base = (
            f"DRIVER={{{self.driver}}}; "
            f"SERVER={self.server}; "
            f"DATABASE={self.database}; "
            f"UID={self.uid};"
        )
        if self.pwd is not None:
            base += f" PWD={self.pwd};"
        if self.trusted_conn:
            base += " Trusted_Connection=yes;"
        if self.trust_server_cert:
            base += " TrustServerCertificate=yes;"
        return base

    __str__ = value

    def __repr__(self):
        return (
            f"{self.__class__.__name__}"
            "("
            f"server={self.server}"
            f", database={self.database}"
            f", uid={self.uid}"
            f", pwd={self.pwd}"
            f", trusted_conn={self.trusted_conn}"
            f", trust_server_cert={self.trust_server_cert}"
            ")"
        )

    def _get_uid(self):
        """
        get current user
        (when `uid` not provided in the class constructor)
        """
        if IS_WINDOWS:
            return "{}\\{}".format(self.server, os.getlogin())
        elif IS_LINUX:
            import pwd as _pwd

            login = _pwd.getpwuid(os.geteuid()).pw_name  # type: ignore
            return "{}\\{}".format(self.server, login)

    @property
    def islocal(self):
        return self.server in [LOCAL_IPv4, LOCAL_HOSTNAME]

    @property
    def isremote(self):
        return not self.islocal

    @property
    def server(self) -> str:
        return self._server

    @server.setter
    def server(self, __srv: str) -> None:
        reset_security_params = hasattr(self, "_server")
        self._server = __srv
        if reset_security_params:
            self._set_security_params(None, None)

    @property
    def database(self) -> str:
        return self._database

    @database.setter
    def database(self, __db: str) -> None:
        self._database = __db

    @property
    def uid(self) -> str:
        return self._uid

    @uid.setter
    def uid(self, __uid: Optional[str]) -> None:
        self._uid = str(__uid or self._get_uid())

    @property
    def pwd(self) -> Optional[str]:
        return self._pwd

    @pwd.setter
    def pwd(self, __pwd: Optional[str]) -> None:
        self._pwd = __pwd

    def _set_security_params(self, trusted_conn, trust_server_cert):
        """
        set security parameters of the connection string

        .. note::

            if neither are provided in the class constructor,
            we check the name of the OS and set parameters accordingly
                if Windows:
                    trusted_conn=True
                    trust_server_cert=False
                if Linux | Mac:
                    trusted_conn=False
                    trust_server_cert=True
        """

        if trusted_conn is None and trust_server_cert is None:
            if IS_WINDOWS:
                if self.islocal and not IS_LINUX:
                    self.trusted_conn = True
                    self.trust_server_cert = False
                else:
                    self.trusted_conn = False
                    self.trust_server_cert = True
            else:
                self.trusted_conn = False
                self.trust_server_cert = True
            return

        if trusted_conn is not None and trusted_conn is not None:
            self.trusted_conn = trusted_conn
            self.trusted_conn = trust_server_cert
            return

        if trusted_conn is not None:
            if trusted_conn is True:
                self.trust_server_cert = False
            else:
                self.trust_server_cert = True
            self.trusted_conn = trusted_conn
        else:
            if trust_server_cert is True:
                self.trusted_conn = False
            else:
                self.trusted_conn = True
            self.trust_server_cert = trust_server_cert

    def to_url(self) -> str:
        if self.islocal:
            if self.uid is not None and self.pwd is not None:
                uid = self.uid.split("\\")[-1]
                uid_pwd = uid + ":" + self.pwd
            else:
                uid_pwd = ""
        else:
            assert isinstance(self.uid, str)
            assert isinstance(self.pwd, str)
            uid_pwd = self.uid + ":" + self.pwd

        dialect_driver = "mssql+pyodbc"
        host = self.server
        port = 1434
        db = self.database

        return f"{dialect_driver}://{uid_pwd}@{host}:{port}/{db}"

    @classmethod
    def from_connection_string(cls, connstring: str):
        # Regular expression to match key-value pairs
        pattern = re.compile(r'(?P<key>[^=;]+)=(?P<value>[^;]*)')

        # Find all matches in the connection string
        matches = pattern.findall(connstring)

        # Create a dictionary from the matches
        params: Dict[str, Optional[str]] = {
            key.strip().lower(): value.strip() for key, value in matches
        }

        for param in (
            "driver",
            "server",
            "database",
            "uid",
            "pwd",
            "trusted_conn",
            "trust_server_cert",
        ):
            if param not in params:
                params[param] = None

        # Convert the dictionary to a SimpleNamespace object
        ns = SimpleNamespace(**params)

        # server: Optional[str] = None,
        # database: Optional[str] = None,
        # uid: Optional[str] = None,
        # pwd: Optional[str] = None,
        # trusted_conn: Optional[bool] = None,
        # trust_server_cert: Optional[bool] = None,
        # *,
        # driver: Optional[str] = None,

        return cls(
            server=ns.server,
            # params["server"],
            database=ns.database,
            # params["database"],
            uid=ns.uid,
            # params["uid"],
            pwd=ns.pwd,
            # params["pwd"],
            # trusted_conn=ns.trusted_conn,
            # trusted_conn=True if params["trusted_conn"] == "yes" else False,
            trusted_conn=True if ns.trusted_conn == "yes" else False,
            # trust_server_cert=ns.trust_server_cert,
            # trust_server_cert=True if params["trust_server_cert"] == "yes" else False,
            trust_server_cert=True if ns.trust_server_cert == "yes" else False,
            driver=ns.driver,
            # driver=params["driver"],
        )

    def __eq__(self, other: "ConnString") -> bool:
        if not isinstance(other, type(self)):
            return False
        if self is other:
            return True
        return self.value() == other.value()
