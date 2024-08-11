class InvalidColumnError(Exception):
    """
    Raised when column information is invalid.
    e.g., when precision and scale parameters
    are missing for a column of type DECIMAL
    """

class SqlException(Exception):
    """Base SqlException"""


class IncompleteParametersException(SqlException):
    """
    Raised when a server parameter is not Provided
    """


class InvalidQueryException(SqlException):
    """
    Raised when a bad query is given for execution
    """


class DatabaseDoesNotExistError(SqlException):
    """
    Raised when a db does not exist
    """

    def __init__(self, db):
        self.db = db
        self.message = f"Database '{db}' does not exist"
        super().__init__(self.message)


class DatabaseOfflineError(SqlException):
    """
    Raised when the status of a db is not 'ONLINE'
    """

    def __init__(self, db, status):
        self.db = db
        self.status = status
        # self.message = f"Database '{db}' is not online (status={status})"
        self.message = f"Database '{db}' status '{status}'"
        super().__init__(self.message)
        self.strerror = self.message


class MSSQLServiceNotStartedError(SqlException):
    """
    Raised when the MSSQL service is not running
    """


class MissingDriverError(SqlException):
    """
    Raised when the required pyodbc driver is not installed
    """

    def __init__(self, driver: str) -> None:
        self.driver = driver
        super().__init__(driver)
