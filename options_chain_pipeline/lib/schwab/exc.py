class InvalidResponseDataError(Exception):
    """
    Raised when data returned by option chain get request is invalid
    """

    # def __init__(self, message: str, errors, sym: str, status_code: int):
    def __init__(self, sym: str, status_code: int):
        self.sym = sym
        self.status_code = status_code
        super().__init__(self.__str__())

    def __str__(self):
        cls_name = type(self).__name__
        return f"{cls_name}(underlying={self.sym}, status={self.status_code})"


class ValidationException(Exception):
    __alias__ = "val"

    def __init__(self, msg, val):
        self.msg = msg
        self.val = val
        super().__init__(self.__str__())

    def __str__(self):
        return f"{self.msg} ({self.__alias__}={self.val})"


class InvalidApiKeyException(ValidationException):
    __alias__ = "apikey"

    # def __init__(self, msg, apikey):
    #     self.msg = msg
    #     self.apikey = apikey
    #     super().__init__(self.__str__())

    # def __str__(self):
    #     # cls_name = type(self).__name__
    #     return "{} ({}={})".format(self.msg, self.__alias__, self.apikey)


class InvalidAcctIdException(ValidationException):
    __alias__ = "acctid"

    # def __init__(self, msg, acct_id):
    #     self.msg = msg
    #     self.acct_id = acct_id
    #     super().__init__(self.__str__())

    # def __str__(self):
    #     # cls_name = type(self).__name__
    #     return f"{self.msg} ({self.__alias__}={self.acct_id})"


class InvalidAccountIndexError(Exception):
    """Raised when an invalid account number is passed"""

    pass


class EmptyLoginCredentialsException(Exception):
    """Raised when either User ID or Password credentials is `None`."""

    pass


class NoCredentialsFoundException(Exception):
    """Raised when no account information can be found within
    environment variables."""
