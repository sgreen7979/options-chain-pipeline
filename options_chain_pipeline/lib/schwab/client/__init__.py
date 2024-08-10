#!/usr/bin/env python3
from options_chain_pipeline.lib.utils.logging import get_logger

from .new import SchwabClient  # noqa
from .group import ClientGroup  # noqa

logger = get_logger(__name__)


def setSchwabClientClass(klass):
    """
    Set the class to be used when instantiating a logger. The class should
    define __init__() such that only a name argument is required, and the
    __init__() should call Logger.__init__()
    """
    if klass != SchwabClient:
        if not issubclass(klass, SchwabClient):
            raise TypeError(
                "schwabClientClass not derived from daily.schwab.client.SchwabClient: "
                + klass.__name__
            )
    global SchwabClient
    SchwabClient = klass


def getSchwabClientClass():
    """
    Return the class to be used when instantiating a logger.
    """
    return SchwabClient



# setSchwabClientClass(SchwabClient)
# SchwabClient = getSchwabClientClass()



