#!/usr/bin/env python3

import datetime
import logging
import logging.config
import logging.handlers
import os
from pathlib import Path
import sys
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Literal
from typing import Optional
from typing import TextIO
from typing import Type
from typing import Union

from daily import LOG_PATH
from daily._types import StrPath

DEFAULT_FORMATTER = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")


# fmt: off
class LogFormatOpts:
    """
    ```
    NAME = "%(name)s"               # Name of the logger (logging channel)
    LEVELNO = "%(levelno)s"         # Numeric logging level for the message (NOTSET=0, DEBUG=10, INFO=20, WARNING=30, ERROR=40, CRITICAL=50)
    LEVELNAME = "%(levelname)s"     # Text logging level for the message ("NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
    PATHNAME = "%(pathname)s"       # Full pathname of the source file where the logging call was issued (if available)
    FILENAME = "%(filename)s"       # Filename portion of pathname
    MODULE = "%(module)s"           # Module (name portion of filename)
    LINENO = "%(lineno)d"           # Source line number where the logging call was issued (if available)
    FUNCNAME = "%(funcName)s"       # Function name
    CREATED = "%(created)f"         # Time when the LogRecord was created (time.time() return value)
    ASCTIME = "%(asctime)s"         # Textual time when the LogRecord was created
    MSECS = "%(msecs)d"             # Millisecond portion of the creation time
    RELATIVECREATED = "%(relativeCreated)d"  # Time in milliseconds when the LogRecord was created, relative to the time the logging module was loaded (typically at application startup time)
    THREAD = "%(thread)d"           # Thread ID (if available)
    THREADNAME = "%(threadName)s"   # Thread name (if available)
    PROCESS = "%(process)d"         # Process ID (if available)
    MESSAGE = "%(message)s"         # The result of record.getMessage(), computed just as
    ```
    """
    NAME = "%(name)s"                           # Name of the logger (logging channel)
    LEVELNO = "%(levelno)s"                     # Numeric logging level for the message (NOTSET=0, DEBUG=10, INFO=20,
                                                # WARNING=30, ERROR=40, CRITICAL=50)
    LEVELNAME = "%(levelname)s"                 # Text logging level for the message ("NOTSET", "DEBUG", "INFO",
                                                # "WARNING", "ERROR", "CRITICAL")
    PATHNAME = "%(pathname)s"                   # Full pathname of the source file where the logging
                                                # call was issued (if available)
    FILENAME = "%(filename)s"                   # Filename portion of pathname
    MODULE = "%(module)s"                       # Module (name portion of filename)
    LINENO = "%(lineno)d"                       # Source line number where the logging call was issued
                                                # (if available)
    FUNCNAME = "%(funcName)s"                   # Function name
    CREATED = "%(created)f"                     # Time when the LogRecord was created (time.time()
                                                # return value)
    ASCTIME = "%(asctime)s"                     # Textual time when the LogRecord was created
    MSECS = "%(msecs)d"                         # Millisecond portion of the creation time
    RELATIVECREATED = "%(relativeCreated)d"     # Time in milliseconds when the LogRecord was created,
                                                # relative to the time the logging module was loaded
                                                # (typically at application startup time)
    THREAD = "%(thread)d"                       # Thread ID (if available)
    THREADNAME = "%(threadName)s"               # Thread name (if available)
    PROCESS = "%(process)d"                     # Process ID (if available)
    MESSAGE = "%(message)s"                     # The result of record.getMessage(), computed just as
# fmt: on                                       # the record is emitted


def ensure_logpath(prefix, suffix, logfolder: Optional[str] = None) -> Path:
    if logfolder is not None:
        while logfolder.endswith("/") or logfolder.endswith("\\"):
            logfolder = logfolder[:-1]
        log_path = Path(LOG_PATH) / logfolder / f"{prefix}{suffix}"
    else:
        log_path = Path(LOG_PATH) / f"{prefix}{suffix}"

    log_path = log_path / f"{datetime.date.today().isoformat()}"
    if not os.path.exists(log_path):
        os.makedirs(log_path, exist_ok=True)
    return log_path


def get_log_filename(
    prefix: str, suffix: str = '', logfolder: Optional[str] = None
) -> str:
    log_dir = ensure_logpath(prefix, suffix, logfolder=logfolder)

    def get_fileno(log_dir):
        if len((listdir := os.listdir(log_dir))):
            return str(
                (
                    max(
                        int(filename.split('_')[-1].split('.')[0])
                        for filename in listdir
                    )
                    + 1
                )
            ).zfill(3)
        return "001"

    fileno = get_fileno(log_dir)
    filename = prefix + suffix + "_" + fileno + '.log'
    return f"{log_dir}\\{filename}"


def format_long_list(it: Iterable, max_log_len: int = 5) -> str:
    it = list(it)
    return (
        ','.join(it[:max_log_len] + ["..."] + it[-1:])
        if len(it) > max_log_len + 1
        else ','.join(it)
    )


def flush_log_handlers(logger: logging.Logger):
    if len(logger.handlers):
        for handler in logger.handlers:
            handler.flush()


def _has_stream_handlers(logger: logging.Logger) -> bool:
    if not logger.hasHandlers():
        return False

    return any(
        isinstance(handler, logging.StreamHandler) for handler in logger.handlers
    )


def _has_file_handlers(logger: logging.Logger) -> bool:
    if not logger.hasHandlers():
        return False

    return any(isinstance(handler, logging.FileHandler) for handler in logger.handlers)


def _get_stream_handler_streams(logger: logging.Logger) -> Optional[List[TextIO]]:
    if not _has_stream_handlers(logger):
        return None
    return [
        stream_handler.stream
        for stream_handler in logger.handlers
        if isinstance(stream_handler, logging.StreamHandler)
    ]


def _get_file_handler_files(logger: logging.Logger) -> Optional[List[StrPath]]:
    if not _has_file_handlers(logger):
        return None
    return [
        file_handler.baseFilename
        for file_handler in logger.handlers
        if isinstance(file_handler, logging.FileHandler)
    ]


def _should_add_new_ch(
    logger: logging.Logger,
    stream: TextIO,
) -> bool:
    if streams := _get_stream_handler_streams(logger):
        for _stream in streams:
            if _stream == stream:
                return False
    return True


def _should_add_new_fh(
    logger: logging.Logger,
    file: StrPath,
) -> bool:
    if files := _get_file_handler_files(logger):
        for _file in files:
            if _file == file:
                return False
    return True

# CRITICAL = 50
# FATAL = CRITICAL
# ERROR = 40
# WARNING = 30
# WARN = WARNING
# INFO = 20
# DEBUG = 10
# NOTSET = 0


LogLevelType = Union[int, Literal["NOTSET", "DEBUG", "INFO", "WARN", "WARNING", "ERROR", "FATAL", "CRITICAL"]]
FileHandlerClassType = Union[
    Type[logging.FileHandler],
    Type[logging.handlers.RotatingFileHandler],
    Type[logging.handlers.TimedRotatingFileHandler],
    Type[logging.handlers.WatchedFileHandler],
    Literal["FileHandler", "RotatingFileHandler", "TimedRotatingFileHandler", "WatchedFileHandler"]
]

_name_to_level = {
    "CRITICAL": 50,
    "FATAL": 50,
    "ERROR": 40,
    "WARNING": 30,
    "WARN": 30,
    "INFO": 20,
    "DEBUG": 10,
    "NOTSET": 0
}

_fhname_to_class: Dict[str, Type["logging.FileHandler"]] = {
    "FileHandler": logging.FileHandler,
    "RotatingFileHandler": logging.handlers.RotatingFileHandler,
    "TimedRotatingFileHandler": logging.handlers.TimedRotatingFileHandler,
    "WatchedFileHandler": logging.handlers.WatchedFileHandler,
}


def get_logger(
    name: Optional[str] = None,
    level: LogLevelType = logging.NOTSET,
    *,
    fmt: Optional[str] = None,
    formatter: Optional[logging.Formatter] = None,
    propagate: bool = True,
    ch: bool = False,
    fh: bool = False,
    ch_stream: Optional[TextIO] = None,
    ch_level: Optional[LogLevelType] = None,
    ch_formatter: Optional[logging.Formatter] = None,
    ch_fmt: Optional[str] = None,
    fh_file: Optional[StrPath] = None,
    fh_level: Optional[LogLevelType] = None,
    fh_formatter: Optional[logging.Formatter] = None,
    fh_fmt: Optional[str] = None,
    # fh_class: Optional[type[logging.FileHandler]] = None,
    fh_type: Optional[FileHandlerClassType] = None,
    fh_type_kwargs: Optional[Dict[str, Any]] = None,
) -> logging.Logger:  # sourcery skip: hoist-if-from-if
    """
    #### LOG LEVELS FOR REFERENCE:
    - CRITICAL = FATAL = 50
    - ERROR = 40
    - WARN = WARNING = 30
    - INFO = 20
    - DEBUG = 10
    - NOTSET = 0
    """
    if isinstance(level, str):
        level = _name_to_level[level]

    logger = logging.getLogger(name)

    if fmt is None and formatter is None:
        formatter = DEFAULT_FORMATTER
    elif formatter is not None:
        formatter = formatter
    else:
        formatter = logging.Formatter(fmt)

    logger.setLevel(level)
    logger.propagate = propagate

    if ch:
        if _should_add_new_ch(logger, ch_stream or sys.stdout):
            new_ch = logging.StreamHandler(ch_stream)
            new_ch.setLevel(ch_level or logger.level)
            if ch_fmt is None and ch_formatter is None:
                new_ch.setFormatter(formatter)
            elif ch_formatter is not None:
                new_ch.setFormatter(ch_formatter)
            else:
                new_ch.setFormatter(logging.Formatter(ch_fmt))
            # new_ch.setFormatter(ch_formatter or formatter)
            logger.addHandler(new_ch)

    if fh:
        fh_file = fh_file or get_log_filename(logger.name)
        if _should_add_new_fh(logger, fh_file):
            if fh_type is None:
                fh_class = logging.FileHandler
            elif isinstance(fh_type, str):
                fh_class = _fhname_to_class[fh_type]
            else:
                fh_class = fh_type
            new_fh = fh_class(fh_file, **(fh_type_kwargs or {}))
            new_fh.setLevel(fh_level or logger.level)
            if fh_fmt is None and fh_formatter is None:
                new_fh.setFormatter(formatter)
            elif fh_formatter is not None:
                new_fh.setFormatter(fh_formatter)
            else:
                new_fh.setFormatter(logging.Formatter(fh_fmt))
            # new_fh.setFormatter(fh_formatter or formatter)
            logger.addHandler(new_fh)

    return logger
