#!/usr/bin/env python3
class EnvException(Exception):
    pass


class ArgumentError(EnvException):
    """
    Raised when an invalid set of arguments is passed to `daily.env.writer`
    """


class NewLevelException(EnvException):
    """
    Raised when there is an error when attempting to add a new write level
    in `daily.env.writer`
    """


class VenvException(EnvException):
    """
    Raised when there is an error creating a venv
    """
