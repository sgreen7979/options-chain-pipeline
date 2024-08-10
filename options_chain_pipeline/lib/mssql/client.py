#!/usr/bin/env python3
from abc import ABC
from abc import abstractmethod
import datetime
from enum import IntEnum
import json
import logging
from threading import RLock
from typing import Any
from typing import cast
from typing import Dict
from typing import List
from typing import Literal
from typing import Optional
from typing import Tuple

import pandas as pd
import pyodbc

from options_chain_pipeline.lib.env import DATA_PATH
from options_chain_pipeline.lib.env import SSH_PASSWORD
from options_chain_pipeline.lib.env import SSH_SERVER
from options_chain_pipeline.lib.env import SSH_USER
from options_chain_pipeline.lib.utils.logging import get_logger

from .bulk_insert import prepare_bulk_insert_query
from .connection_string import ConnString
from .utils import LocalTempHandler
from .utils import RemoteTempHandler
from .protocols import Connection as ConnectionProto

logger = get_logger("MSSQLClient", level=logging.DEBUG, propagate=False, fh=True)


class AbstractSqlClient(ABC):

    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def connect(self): ...

    @property
    def conn(self) -> ConnectionProto:
        if not hasattr(self, "_conn"):
            self.connect()
        return self._conn

    def __enter__(self): ...

    def __exit__(self, exc_type, exc_val, exc_tb): ...

    @conn.setter
    def conn(self, conn):
        self._conn = conn

    @abstractmethod
    def execute(self, *args, **kwargs): ...

    @abstractmethod
    def scalar(self, *args, **kwargs): ...

    @classmethod
    @abstractmethod
    def from_url(cls, url): ...



class MSSQLClient(AbstractSqlClient):
    """MSSQLServer database client.

    Suggested usage:
        ```python

        from daily.sql.mssql.client import MSSQLClient
        from daily.sql.mssql.config import MSSQLConfig
    """

    _num_connnected: int = 0
    _num_conn_lock: RLock = RLock()

    def __init__(
        self,
        cstring: ConnString,
        *,
        autocommit: bool = False,
    ) -> None:
        """Initialize the client.

        :param cstring: the connection string object
        :type cstring: ConnString
        :param autocommit: autocommit inserts or updates, defaults to False
        :type autocommit: bool, optional
        """
        super().__init__()
        self._cstring_obj = cstring
        self.autocommit = autocommit
        self.temphandler = self._get_temphandler()
        self._entered: bool = False
        logger.info(f"init @ {hex(id(self))}")
        logger.info(f"{self.conn_str} autocommit={self.autocommit}")

    @property
    def is_local(self) -> bool:
        return self._cstring_obj.islocal

    @property
    def is_remote(self) -> bool:
        return self._cstring_obj.isremote

    @staticmethod
    def get_logger() -> logging.Logger:
        return logger

    def _get_temphandler(self):
        today = datetime.date.today().isoformat()
        local_backup_path = f"{DATA_PATH}/bulk_insert_debug/{today}"

        if self.is_remote:
            assert isinstance(SSH_SERVER, str)
            assert isinstance(SSH_USER, str)
            assert isinstance(SSH_PASSWORD, str)
            logger.info("initializing RemoteTempHandler")
            return RemoteTempHandler(
                SSH_SERVER,
                SSH_USER,
                SSH_PASSWORD,
                local_backup_path,
                "D:\\test_bulk_insert",
            )
        logger.info("initializing LocalTempHandler")
        return LocalTempHandler(local_backup_path)

    def commit(self):
        self.conn.commit()

    def cursor(self):
        return self.conn.cursor()

    def get_max_connections(self) -> int:
        return self.scalar("SELECT @@MAX_CONNECTIONS;")

    @property
    def database(self) -> str:
        return self._cstring_obj.database

    @database.setter
    def database(self, __db: str) -> None:
        logger.info(f"changing database to {__db}")
        self._cstring_obj.database = __db

    @property
    def server(self):
        return self._cstring_obj.server

    @server.setter
    def server(self, __srv: str) -> None:
        logger.info(f"changing server to {__srv}")
        self._cstring_obj.server = __srv
        # # TODO: handle this hear or in the ConnString class?
        # # Reset the security params if we're changing the server
        # self._cstring_obj._set_security_params(None, None)

    @property
    def uid(self) -> str:
        return cast(str, self._cstring_obj.uid)

    @uid.setter
    def uid(self, __uid: str) -> None:
        logger.info(f"changing uid to {__uid}")
        self._cstring_obj.uid = __uid

    @property
    def pwd(self) -> Optional[str]:
        return self._cstring_obj.pwd

    @pwd.setter
    def pwd(self, __pwd: Optional[str]) -> None:
        logger.info("changing password ****")
        self._cstring_obj.pwd = __pwd

    @property
    def connected(self) -> bool:
        if hasattr(self, "_conn") and self.conn is not None:
            return not getattr(self.conn, "closed")
        return False

    @property
    def closed(self) -> bool:
        if not hasattr(self, "_conn") or self.conn is None:
            return True
        return getattr(self.conn, "closed")

    @property
    def conn_str(self) -> str:
        return self._cstring_obj.value()

    def connect(self) -> None:
        if not self.connected:
            try:
                logger.info("connecting")
                self.conn = pyodbc.connect(self.conn_str, autocommit=self.autocommit)
                MSSQLClient.register_connection()
                # logging.info("Connected to database.")
            except pyodbc.Error as e:
                # logging.error(f"Error connecting to database: {e}")
                logger.error(f"error connecting to database: {e}", exc_info=True)
                # print(f"Error connecting to database: {e}")
                raise

    def close(self) -> None:
        if not self.closed:
            logger.debug("closing")
            self.conn.close()
            MSSQLClient.register_closing()

    @property
    def conn(self) -> pyodbc.Connection:
        if not hasattr(self, "_conn"):
            self.connect()
        return self._conn

    @conn.setter
    def conn(self, conn) -> None:
        self._conn = conn

    def bulk_insert(
        self,
        lines: List[str],
        table: str,
        firstrow: int = 2,
        fieldterminator: Literal[',', "|", ";", "\\t", "\\s"] = ',',
        rowterminator: Literal['0x0A', "\\n"] = '\\n',
        lastrow: Optional[int] = None,
        maxerrors: int = int(1e9),
        fire_triggers: bool = False,
    ):
        with self.temphandler as th:
            fpath = th.write(lines)
            sql = prepare_bulk_insert_query(
                fpath=fpath,
                table=table,
                firstrow=firstrow,
                fieldterminator=fieldterminator,
                rowterminator=rowterminator,
                lastrow=lastrow or len(lines),
                maxerrors=maxerrors,
                fire_triggers=fire_triggers,
            )
            logger.info(f"Bulk inserting data from remote path {fpath}")
            self.execute(sql)

    def _recursive_json_encode(self, data: dict):
        for key, value in data.items():
            if isinstance(value, (str, int, float)):
                continue
            if isinstance(value, dict):
                data[key] = self._recursive_json_encode(value)
            elif isinstance(value, (datetime.date, datetime.datetime)):
                data[key] = value.isoformat()
        return data

    def insert_json_list(
        self,
        data: List[Dict[str, Any]],
        table: str,
        catalog: str,
        schema: str,
        json_column_name: str,
        metadata: Optional[Dict[str, Any]] = None,
        *,
        stored_procedures: Optional[List[str]] = None,
    ):
        json_string = json.dumps(data)
        keys = [json_column_name]
        values = [json_string]
        if metadata is not None:
            keys.extend(list(metadata.keys()))
            values.extend(list(metadata.values()))
        keys_str = ", ".join(str(key) for key in keys)
        len_values = len(keys)
        insert_query = f"""
        INSERT INTO {catalog}.{schema}.{table} {keys_str}
        VALUES ({','.join("?" for _ in range(len_values))})
        """
        self.execute(insert_query, *values)

    def rename_column(
        self,
        table: str,
        old_column: str,
        new_column: str,
        catalog: str,
        schema: str = "dbo",
    ):
        rename_query = f"""USE {catalog};
        EXEC sp_rename '{schema}.{table}.{old_column}', '{new_column}', 'COLUMN';
        """
        self.execute(rename_query)

    def alter_column(
        self,
        table: str,
        column: str,
        new_def: str,
        catalog: str,
        schema: str = "dbo",
    ):
        """
        e.g. new_def="float NOT NULL"
                    ="datetime2(6) DEFAULT GET_DATE()"
        """
        alter_column_query = f"""
        ALTER TABLE {catalog}.{schema}.{table}
        ALTER COLUMN {column} {new_def};
        """
        self.execute(alter_column_query)

    def add_column(
        self,
        table: str,
        column: str,
        col_def: str,
        catalog: str,
        schema: str = "dbo",
    ):
        self.execute(f"ALTER TABLE {catalog}.{schema}.{table} ADD {column} {col_def};")

    def get_pk_info(self, table: str):
        with self.conn.cursor() as cur:
            cur = cur.execute(
                f"SELECT FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = '{table}'"
            )
            cur, results = self._extract_cursor_results(cur)
        return results

    def __enter__(self) -> "MSSQLClient":
        if self._entered:
            raise RuntimeError("already entered, cannot re-enter")
        logger.debug("entering")
        self._entered = True
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        logger.debug("exiting")
        self._entered = False
        self.close()

    def select(self, sql, *params):
        logger.debug(f"executing select {sql}, {params}")
        with self.conn.cursor() as cur:
            cur = cur.execute(sql, *params)
            cur, results = self._extract_cursor_results(cur)
        return results

    def execute(self, sql: str, *params):
        logger.debug(f"executing command {sql}, {params}")
        with self.conn.cursor() as cur:
            return cur.execute(sql, *params)

    def scalar(self, sql: str, *params):
        logger.debug(f"executing scalar {sql}, {params}")
        with self.conn.cursor() as cur:
            return cur.execute(sql, *params).fetchval()

    def _extract_cursor_results(
        self, cur: pyodbc.Cursor
    ) -> Tuple[pyodbc.Cursor, List[Dict[str, Any]]]:
        logger.debug(f"extracting cursor results from cur {cur.description}")
        columns = [column[0] for column in cur.description]
        results = cur.fetchall()
        ret: List[Dict[str, Any]] = []
        for row in results:
            row_dict = {}
            for idx, col in enumerate(columns):
                row_dict[col] = row[idx]
            ret.append(row_dict)
        return cur, ret

    def get_table_definitions(
        self, table: str, catalog: str, schema: str = "dbo", to_df: bool = False
    ):
        # with sql_client as sql_client:
        # with sql_client.conn.cursor() as cur:
        logger.info(f"getting table definitions for [{catalog}].[{schema}].[{table}]")
        with self.conn.cursor() as cur:
            cur = cur.columns(table=table, catalog=catalog, schema=schema)
            columns = [column[0] for column in cur.description]
            results = cur.fetchall()

        if to_df:
            df_dict = {}
            for idx, col in enumerate(columns):
                df_dict[col] = [row[idx] for row in results]
            ret = pd.DataFrame(df_dict)
        else:
            ret = []
            for row in results:
                row_dict = {}
                for idx, col in enumerate(columns):
                    row_dict[col] = row[idx]
                ret.append(row_dict)
        return ret

    def create_table(
        self, table_name: str, col_info, db: str = "master", schema: str = "dbo"
    ) -> None:
        # sql = f"""
        # CREATE TABLE [{db}].[{schema}].[{table_name}] (

        # )
        # """
        pass

    def __repr__(self) -> str:
        status = f"Connected @ '{self.conn_str}'" if self.connected else "Closed"
        return f"<{self.__class__.__name__} {status}>"

    @classmethod
    def register_connection(cls) -> None:
        with cls._num_conn_lock:
            cls._num_connnected += 1

    @classmethod
    def register_closing(cls) -> None:
        with cls._num_conn_lock:
            cls._num_connnected -= 1

    @classmethod
    def get_num_connected(cls) -> int:
        with cls._num_conn_lock:
            num_connected = cls._num_connnected
        return num_connected

    @classmethod
    def from_url(cls, url):
        pass

    @classmethod
    def from_connection_params(
        cls,
        server: Optional[str] = None,
        database: Optional[str] = None,
        uid: Optional[str] = None,
        pwd: Optional[str] = None,
        trusted_conn: Optional[bool] = None,
        trust_server_cert: Optional[bool] = None,
        *,
        autocommit: bool = False,
    ) -> "MSSQLClient":
        return cls(
            ConnString(
                server,
                database,
                uid,
                pwd,
                trusted_conn=trusted_conn,
                trust_server_cert=trust_server_cert,
            ),
            autocommit=autocommit,
        )
