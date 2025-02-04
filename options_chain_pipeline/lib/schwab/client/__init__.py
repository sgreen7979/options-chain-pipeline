#!/usr/bin/env python3
from .new import SchwabClient  # noqa
from .group import ClientGroup  # noqa


def setSchwabClientClass(klass):
    """
    Set the class to be used when instantiating a logger. The class should
    define __init__() such that only a name argument is required, and the
    __init__() should call Logger.__init__()
    """
    if klass != SchwabClient:
        if not issubclass(klass, SchwabClient):
            raise TypeError(
                "schwabClientClass not derived from options_chain_pipeline.lib.schwab.client.SchwabClient: "
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



