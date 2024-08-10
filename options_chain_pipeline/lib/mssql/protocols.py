#!/usr/bin/env python3
from __future__ import annotations

from typing import Any
from typing import Callable
from typing import Generator
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import Protocol
from typing import Sequence
from typing import Tuple
from typing import TYPE_CHECKING
from typing import Union

if TYPE_CHECKING:
    from .connection_string import ConnString


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

#!/usr/bin/env python3



class Row(Protocol):
    """The class representing a single record in the result set from a query.  Objects of
    this class behave somewhat similarly to a NamedTuple.  Column values can be accessed
    by column name (i.e. using dot notation) or by row index.
    """

    @property
    def cursor_description(self) -> Tuple[Tuple[str, Any, int, int, int, int, bool]]:
        """The metadata for the columns in this Row, as retrieved from the parent Cursor object."""
        ...

    # implemented dunder methods
    def __contains__(self, key, /) -> int: ...
    def __delattr__(self, name, /) -> None: ...
    def __delitem__(self, key, /) -> None: ...
    def __eq__(self, value, /) -> bool: ...
    def __ge__(self, value, /) -> bool: ...
    def __getattribute__(self, name, /) -> Any: ...
    def __getitem__(self, key, /) -> Any: ...
    def __gt__(self, value, /) -> bool: ...
    def __iter__(self) -> Iterator[Any]: ...
    def __le__(self, value, /) -> bool: ...
    def __len__(self, /) -> int: ...
    def __lt__(self, value, /) -> bool: ...
    def __ne__(self, value, /) -> bool: ...
    def __reduce__(self) -> Any: ...
    def __repr__(self, /) -> str: ...
    def __setattr__(self, name, value, /) -> None: ...
    def __setitem__(self, key, value, /) -> None: ...


class Connection(Protocol):
    """The ODBC connection class representing an ODBC connection to a database, for
    managing database transactions and creating cursors.
    https://www.python.org/dev/peps/pep-0249/#connection-objects

    This class should not be instantiated directly, instead call pyodbc.connect() to
    create a Connection object.
    """

    @property
    def autocommit(self) -> bool:
        """Whether the database automatically executes a commit after every successful transaction.
        Default is False.
        """
        ...

    @autocommit.setter
    def autocommit(self, value: bool) -> None: ...

    @property
    def maxwrite(self) -> int:
        """The maximum bytes to write before using SQLPutData, default is zero for no maximum."""
        ...

    @maxwrite.setter
    def maxwrite(self, value: int) -> None: ...

    @property
    def searchescape(self) -> str:
        """The character for escaping search pattern characters like "%" and "_".
        This is typically the backslash character but can be driver-specific."""
        ...

    @property
    def timeout(self) -> int:
        """The timeout in seconds for SQL queries, use zero (the default) for no timeout limit."""
        ...

    @timeout.setter
    def timeout(self, value: int) -> None: ...

    def __enter__(self) -> Connection: ...
    def __exit__(self, exc_type, exc_value, traceback) -> None: ...

    # functions for defining the text encoding used for data, metadata, sql, parameters, etc.

    def setencoding(
        self, encoding: Optional[str] = None, ctype: Optional[int] = None
    ) -> None:
        """Set the text encoding for SQL statements and textual parameters sent to the database.

        Args:
            encoding: Text encoding codec, e.g. "utf-8".
            ctype: The C data type when passing data - either pyodbc.SQL_CHAR or pyodbc.SQL_WCHAR.  More
                relevant for Python 2.7.
        """
        ...

    def setdecoding(
        self, sqltype: int, encoding: Optional[str] = None, ctype: Optional[int] = None
    ) -> None:
        """Set the text decoding used when reading SQL_CHAR or SQL_WCHAR data from the database.

        Args:
            sqltype: pyodbc.SQL_CHAR, pyodbc.SQL_WCHAR, or pyodbc.SQL_WMETADATA.
            encoding: Text encoding codec, e.g. "utf-8".
            ctype: The C data type to request from SQLGetData - either pyodbc.SQL_CHAR or
                pyodbc.SQL_WCHAR.  More relevant for Python 2.7.
        """
        ...

    # functions for getting/setting connection attributes

    def getinfo(self, infotype: int, /) -> Any:
        """Retrieve general information about the driver and the data source, via SQLGetInfo.

        Args:
            infotype: Id of the information to retrieve.

        Returns:
            The value of the requested information.
        """
        ...

    def set_attr(self, attr_id: int, value: int, /) -> None:
        """Set an attribute on the connection, via SQLSetConnectAttr.

        Args:
            attr_id: Id for the attribute, as defined by ODBC or the driver.
            value: The value of the attribute.
        """
        ...

    # functions to handle non-standard database data types

    def add_output_converter(self, sqltype: int, func: Optional[Callable], /) -> None:
        """Register an output converter function that will be called whenever a value
        with the given SQL type is read from the database.  See the Wiki for details:
        https://github.com/mkleehammer/pyodbc/wiki/Using-an-Output-Converter-function

        Args:
            sqltype: The SQL type for the values to convert.
            func: The converter function.
        """
        ...

    def get_output_converter(self, sqltype: int, /) -> Optional[Callable]:
        """Retrieve the (previously registered) converter function for the SQL type.

        Args:
            sqltype: The SQL type.

        Returns:
            The converter function if it exists, None otherwise.
        """
        ...

    def remove_output_converter(self, sqltype: int, /) -> None:
        """Delete a previously registered output converter function.

        Args:
            sqltype: The SQL type.
        """
        ...

    def clear_output_converters(self) -> None:
        """Delete all previously registered converter functions."""
        ...

    # functions for managing database transactions (in typical order of use)

    def cursor(self) -> Cursor:
        """Create a new cursor on the connection.

        Returns:
            A new cursor.
        """
        ...

    def execute(self, sql: str, *params: Any) -> Cursor:
        """A convenience function for running queries directly from a Connection object.
        Creates a new cursor, runs the SQL query, and returns the new cursor.

        Args:
            sql: The SQL query.
            *params: Any parameter values for the SQL query.

        Returns:
            A new cursor.
        """
        ...

    def commit(self) -> None:
        """Commit all SQL statements executed on the connection since the last commit/rollback."""
        ...

    def rollback(self) -> None:
        """Rollback all SQL statements executed on the connection since the last commit/rollback."""
        ...

    def close(self) -> None:
        """Close the connection.  Any uncommitted SQL statements will be rolled back."""
        ...


class Cursor(Protocol):
    """The class representing database cursors.  Cursors are vehicles for executing SQL
    statements and returning their results.
    https://www.python.org/dev/peps/pep-0249/#cursor-objects

    This class should not be instantiated directly, instead call cursor() from a Connection
    object to create a Cursor object.
    """

    @property
    def arraysize(self) -> int:
        """The number of rows at a time to fetch with fetchmany(), default is 1."""
        ...

    @arraysize.setter
    def arraysize(self, value: int) -> None: ...

    @property
    def connection(self) -> Connection:
        """The parent Connection object for the cursor."""
        ...

    @property
    def description(self) -> Tuple[Tuple[str, Any, int, int, int, int, bool]]:
        """The metadata for the columns returned in the last SQL SELECT statement, in
        the form of a list of tuples.  Each tuple contains seven fields:

        0. name of the column (or column alias)
        1. type code, the Python-equivalent class of the column, e.g. str for VARCHAR
        2. display size (pyodbc does not set this value)
        3. internal size (in bytes)
        4. precision
        5. scale
        6. nullable (True/False)

        ref: https://peps.python.org/pep-0249/#description
        """
        ...

    @property
    def fast_executemany(self) -> bool:
        """When this cursor property is False (the default), calls to executemany() do
        nothing more than iterate over the provided list of parameters and calls execute()
        on each set of parameters.  This is typically slow.  When fast_executemany is
        True, the parameters are sent to the database in one bundle (with the SQL).  This
        is usually much faster, but there are limitations.  Check the Wiki for details.
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#executemanysql-params-with-fast_executemanytrue
        """
        ...

    @fast_executemany.setter
    def fast_executemany(self, value: bool) -> None: ...

    @property
    def messages(self) -> Optional[List[Tuple[str, Union[str, bytes]]]]:
        """Any descriptive messages returned by the last call to execute(), e.g. PRINT
        statements, or None."""
        ...

    @property
    def noscan(self) -> bool:
        """Whether the driver should scan SQL strings for escape sequences, default is True."""
        ...

    @noscan.setter
    def noscan(self, value: bool) -> None: ...

    @property
    def rowcount(self) -> int:
        """The number of rows modified by the last SQL statement.  Has the value of -1
        if the number of rows is unknown or unavailable.
        """
        ...

    # implemented dunder methods
    def __enter__(self) -> Cursor: ...
    def __exit__(self, exc_type, exc_value, traceback) -> None: ...
    def __iter__(self, /) -> Cursor: ...
    def __next__(self, /) -> Row: ...

    # functions for running SQL queries (in rough order of use)

    def setinputsizes(self, sizes: Optional[Iterable[Tuple[int, int, int]]], /) -> None:
        """Explicitly declare the types and sizes of the parameters in a query.  Set
        to None to clear any previously registered input sizes.

        Args:
            sizes: A list of tuples, one tuple for each query parameter, where each
                tuple contains:

                    1. the column datatype
                    2. the column size (char length or decimal precision)
                    3. the decimal scale.

                For example: [(pyodbc.SQL_WVARCHAR, 50, 0), (pyodbc.SQL_DECIMAL, 18, 4)]
        """
        ...

    def setoutputsize(self) -> None:
        """Not supported."""
        ...

    def execute(self, sql: str, *params: Any) -> Cursor:
        """Run a SQL query and return the cursor.

        Args:
            sql: The SQL query.
            *params: Any parameters for the SQL query, as positional arguments or a single iterable.

        Returns:
            The cursor, so that calls on the cursor can be chained.
        """
        ...

    def executemany(
        self, sql: str, params: Union[Sequence, Iterator, Generator], /
    ) -> None:
        """Run the SQL query against an iterable of parameters.  The behavior of this
        function depends heavily on the setting of the fast_executemany cursor property.
        See the Wiki for details.
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#executemanysql-params-with-fast_executemanyfalse-the-default
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#executemanysql-params-with-fast_executemanytrue

        Args:
            sql: The SQL query.
            *params: Any parameters for the SQL query, as an iterable of parameter sets.
        """
        ...

    def fetchone(self) -> Optional[Row]:
        """Retrieve the next row in the current result set for the query.

        Returns:
            A row of results, or None if there is no more data to return.
        """
        ...

    def fetchmany(self, size: int, /) -> List[Row]:
        """Retrieve the next rows in the current result set for the query, as a list.

        Args:
            size: The number of rows to return.

        Returns:
            A list of rows, or an empty list if there is no more data to return.
        """
        ...

    def fetchall(self) -> List[Row]:
        """Retrieve all the remaining rows in the current result set for the query, as a list.

        Returns:
            A list of rows, or an empty list if there is no more data to return.
        """
        ...

    def fetchval(self) -> Any:
        """A convenience function for returning the first column of the first row from
        the query.

        Returns:
            The value in the first column of the first row, or None if there is no data.
        """
        ...

    def skip(self, count: int, /) -> None:
        """Skip over rows in the current result set of a query.

        Args:
            count: The number of rows to skip.
        """
        ...

    def nextset(self) -> bool:
        """Switch to the next result set in the SQL query (e.g. if there are
        multiple SELECT statements in the SQL script).

        Returns:
            True if there are more result sets, False otherwise.
        """
        ...

    def commit(self) -> None:
        """Commit all SQL statements executed on the parent connection since the last
        commit/rollback.  Note, this affects ALL cursors on the parent connection.
        Hence, consider calling commit() on the parent Connection object instead.
        """
        ...

    def rollback(self) -> None:
        """Rollback all SQL statements executed on the parent connection since the last
        commit/rollback.  Note, this affects ALL cursors on the parent connection.
        Hence, consider calling rollback() on the parent Connection object instead.
        """
        ...

    def cancel(self) -> None:
        """Cancel the processing of the current query.  Typically this has to be called
        from a separate thread.
        """
        ...

    def close(self) -> None:
        """Close the cursor, discarding any remaining result sets and/or messages."""
        ...

    # functions to retrieve database metadata

    def tables(
        self,
        table: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        tableType: Optional[str] = None,
    ) -> Cursor:
        """Return information about tables in the database, typically from the
        INFORMATION_SCHEMA.TABLES metadata view.  Parameter values can include
        wildcard characters.

        Args:
            table: Name of the database table.
            catalog: Name of the catalog (database).
            schema: Name of the table schema.
            tableType: Kind of table, e.g. "BASE TABLE".

        Returns:
            The cursor object, containing table information in the result set.
        """
        ...

    def columns(
        self,
        table: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        column: Optional[str] = None,
    ) -> Cursor:
        """Return information about columns in database tables, typically from the
        INFORMATION_SCHEMA.COLUMNS metadata view.  Parameter values can include
        wildcard characters.

        Args:
            table: Name of the database table.
            catalog: Name of the catalog (database).
            schema: Name of the table schema.
            column: Name of the column in the table.

        Returns:
            The cursor object, containing column information in the result set.
        """
        ...

    def statistics(
        self,
        table: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        unique: bool = False,
        quick: bool = True,
    ) -> Cursor:
        """Return statistical information about database tables.  Parameter values
        can include wildcard characters.

        Args:
            table: Name of the database table.
            catalog: Name of the catalog (database).
            schema: Name of the table schema.
            unique: If True, include information about unique indexes only, not all indexes.
            quick: If True, CARDINALITY and PAGES are returned only if they are readily
                available, otherwise None is returned for them.

        Returns:
            The cursor object, containing statistical information in the result set.
        """
        ...

    def rowIdColumns(
        self,
        table: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        nullable: bool = True,
    ) -> Cursor:
        """Return the column(s) in a database table that uniquely identify each row
        (e.g. the primary key column).  Parameter values can include wildcard characters.

        Args:
            table: Name of the database table.
            catalog: Name of the catalog (database).
            schema: Name of the table schema.
            nullable: If True, include sets of columns that are nullable.

        Returns:
            The cursor object, containing the relevant column information in the result set.
        """
        ...

    def rowVerColumns(
        self,
        table: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        nullable: bool = True,
    ) -> Cursor:
        """Return the column(s) in a database table that are updated whenever the row
        is updated.  Parameter values can include wildcard characters.

        Args:
            table: Name of the database table.
            catalog: Name of the catalog (database).
            schema: Name of the table schema.
            nullable: If True, include sets of columns that are nullable.

        Returns:
            The cursor object, containing the relevant column information in the result set.
        """
        ...

    def primaryKeys(
        self, table: str, catalog: Optional[str] = None, schema: Optional[str] = None
    ) -> Cursor:
        """Return the column(s) in a database table that make up the primary key on
        the table.  Parameter values can include wildcard characters.

        Args:
            table: Name of the database table.
            catalog: Name of the catalog (database).
            schema: Name of the table schema.

        Returns:
            The cursor object, containing primary key information in the result set.
        """
        ...

    def foreignKeys(
        self,
        table: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        foreignTable: Optional[str] = None,
        foreignCatalog: Optional[str] = None,
        foreignSchema: Optional[str] = None,
    ) -> Cursor:
        """Return the foreign keys in a database table, i.e. any columns that refer to
        primary key columns on another table.  Parameter values can include wildcard characters.

        Args:
            table: Name of the database table.
            catalog: Name of the catalog (database).
            schema: Name of the table schema.
            foreignTable: Name of the foreign database table.
            foreignCatalog: Name of the foreign catalog (database).
            foreignSchema: Name of the foreign table schema.

        Returns:
            The cursor object, containing foreign key information in the result set.
        """
        ...

    def procedures(
        self,
        procedure: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> Cursor:
        """Return information about stored procedures.  Parameter values can include
        wildcard characters.

        Args:
            procedure: Name of the stored procedure.
            catalog: Name of the catalog (database).
            schema: Name of the table schema.

        Returns:
            The cursor object, containing stored procedure information in the result set.
        """
        ...

    def procedureColumns(
        self,
        procedure: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> Cursor:
        """Return information about the columns used as input/output parameters in
        stored procedures.  Parameter values can include wildcard characters.

        Args:
            procedure: Name of the stored procedure.
            catalog: Name of the catalog (database).
            schema: Name of the table schema.

        Returns:
            The cursor object, containing stored procedure column information in the result set.
        """
        ...

    def getTypeInfo(self, sqlType: Optional[int] = None, /) -> Cursor:
        """Return information about data types supported by the data source.

        Args:
            sqlType: The SQL data type.

        Returns:
            The cursor object, containing information about the SQL data type in the result set.
        """
        ...

