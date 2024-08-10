#!/usr/bin/env python3

from contextlib import contextmanager
import logging
import os
from typing import Any
from typing import Dict
from typing import Generator
from typing import List
import winreg

from dotenv import load_dotenv
import pyodbc
import win32service
import win32serviceutil

from daily.env import MODULE
from daily.env import SQL_PRIMARY_SERVER
from daily.sql.mssql.connection_string import ConnString
from daily.utils.logging import get_logger

# from connection_string import cstring

# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
# formatter = logging.Formatter(
#     "[%(asctime)s][%(levelname)s][%(process)d][%(threadName)s]"
#     "[%(filename)s][%(lineno)d]:%(message)s"
# )
# ch = logging.StreamHandler()
# ch.setFormatter(formatter)
# ch.setLevel(logging.DEBUG)
# logger.addHandler(ch)

logger = get_logger(__name__, ch=True)

dotenv_path = f"./{MODULE}/sql/.env"
if os.path.isfile(dotenv_path):
    returncode = load_dotenv(dotenv_path)
    assert returncode, logger.error(  # type: ignore
        f"Failed to load dotenv from {dotenv_path}"
    )

SERVER = os.environ["SQL_PRIMARY_SERVER"]
# USER = os.environ["USER"]
UID = os.environ["SQL_UID"]
PWD = os.environ["SQL_PWD"]
# USERNAME = "{}\\{}".format(SERVER, USER)
# DATABASE = os.environ["DATABASE"]
# DATABASE = 'master'

CSTRING = ConnString(
    server=SERVER,
    # database=DATABASE,
    uid=UID,
    pwd=PWD,
    # trusted_conn=True,
    # trust_server_cert=True
)
# print(CSTRING.value())


def is_sql_server_running():
    try:
        with winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            # r"SOFTWARE\Microsoft\Microsoft SQL Server"
            "SYSTEM\\CurrentControlSet\\Services\\MSSQLSERVER",
        ) as service_key:
            startup_type, service_status = winreg.QueryValueEx(service_key, "Start")
            return service_status == win32service.SERVICE_RUNNING

    except WindowsError:
        return False


def is_sql_server_running_alt():
    try:
        service_name = "MSSQLSERVER"
        service_status = win32serviceutil.QueryServiceStatus(serviceName=service_name)[
            1
        ]
        return service_status == win32service.SERVICE_RUNNING

    except WindowsError:
        return False


def start_mssql_service():
    if not is_sql_server_running() and not is_sql_server_running_alt():
        service_name = "MSSQLSERVER"
        try:
            # Attempt to start the service.
            win32serviceutil.StartService(service_name)
            logger.info(f"Service {service_name} started successfully.")
            return 0
        except Exception as e:
            logger.error(
                f"Failed to start service {service_name}: {str(e)}", exc_info=True
            )
            return 1


def _listrow_todict(data: List[pyodbc.Row]) -> Dict[str, Any]:
    res = {}
    for k, v in data:
        res[k] = v
    return res


def get_db_statuses(cstring: ConnString = CSTRING):
    with create_server_connection(cstring=cstring) as cnxn:
        statuses = cnxn.execute("select name, state_desc from sys.databases").fetchall()
    return _listrow_todict(statuses)


def get_db_status(db):
    db_statuses = get_db_statuses()
    if db not in db_statuses.keys():
        from daily.sql.exc import DatabaseDoesNotExistError

        raise DatabaseDoesNotExistError(db=db)
    return db_statuses[db]


def create_server_connection_OLD(cstring=CSTRING, autocommit: bool = True):
    """
    Provides a database server connection
    via pyodbc.connect directly
    """

    cnxn = None  # closes existing connections
    try:
        cnxn = pyodbc.connect(cstring.value(), autocommit=autocommit)
        return cnxn
    except pyodbc.OperationalError as e:
        logger.error(e, exc_info=True)
    except pyodbc.InterfaceError:
        if is_sql_server_running():
            db = cstring.database
            # if db_status != "ONLINE":
            if (db_status := get_db_status(db)) != "ONLINE":
                from daily.sql.exc import DatabaseOfflineError

                raise DatabaseOfflineError(db, db_status)
            # logger.error(
            #     f"{type(e).__name__} raised, but the MSSQLSERVER service "
            #     "is running. Check if the issue is related to the "
            #     "database parameter of the cstring provided "
            #     f"(cstring={cstring})"
            # )
        else:
            from daily.sql.exc import MSSQLServiceNotStartedError

            raise MSSQLServiceNotStartedError
            # logger.error(e, exc_info=True)


@contextmanager
def create_server_connection(
    cstring: ConnString = CSTRING, autocommit: bool = True
) -> Generator[pyodbc.Connection, Any, None]:
    conn = pyodbc.connect(cstring.value(), autocommit=autocommit)
    try:
        assert (conn is not None) and (
            not getattr(conn, "closed")
        ), f"Failed to connect to server {str(cstring)}"
        yield conn
    except pyodbc.OperationalError as e:
        logger.error(e, exc_info=True)
    except pyodbc.InterfaceError as e:
        if is_sql_server_running():
            db = cstring.database
            if (db_status := get_db_status(db)) != "ONLINE":
                from daily.sql.exc import DatabaseOfflineError

                raise DatabaseOfflineError(db, db_status) from e
        else:
            from daily.sql.exc import MSSQLServiceNotStartedError

            raise MSSQLServiceNotStartedError() from e
    else:
        if not conn.closed:  # type: ignore
            conn.close()


# patch for dash component
db_conn = create_server_connection


def sql_import(*args, **kwargs):

    with db_conn() as conn:

        try:
            conn.execute(*args, **kwargs)

        except pyodbc.IntegrityError:
            pass

        except pyodbc.OperationalError as e:
            logger.error(e, exc_info=True)

        except pyodbc.ProgrammingError as e:
            logger.error(e, exc_info=True)
            logger.error(f"args={args}\nkwargs={kwargs:!r}")
