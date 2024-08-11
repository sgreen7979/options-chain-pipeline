#!/usr/bin/env python3
from typing import Any
from typing import Dict



class _Singleton(type):
    """
    A metaclass for singleton purpose. Every singleton class should inherit
    from this class by 'metaclass=Singleton'.
    """

    _instances: dict = {}

    def __call__(cls, *args, **kwargs):
        # sourcery skip: instance-method-first-arg-name
        if cls not in cls._instances:
            cls._instances[cls] = super(_Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class StrictSingleton(type):

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls in Singleton._instances:
            return Singleton._instances[cls]
        else:
            obj = super().__call__(*args, **kwargs)
            Singleton._instances[cls] = obj
            return obj

    def _clear_cache(cls):
        if cls in StrictSingleton._instances:
            del StrictSingleton._instances[cls]


class Singleton(type):
    """
    A metaclass for singleton purpose. Every singleton class should inherit
    from this class by 'metaclass=Singleton'.
    """

    _instances: Dict[Any, Any] = {}

    def __call__(cls, *args, **kwargs):
        """
        Create or return the instance of the singleton class.

        Args:
            *args: The arguments passed to the constructor.
            **kwargs: The keyword arguments passed to the constructor.

        Returns:
            object: The singleton instance.
        """
        key = (args, tuple(sorted(kwargs.items())))
        # if no instances of the class exist
        if cls not in cls._instances:
            # add a new dictionary to _instances for the cls,
            # to which future instances of cls will be recorded
            cls._instances[cls] = {}
        if key not in cls._instances[cls]:
            # cls._instances[cls][key] = super(Singleton, cls).__call__(*args, **kwargs)
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls][key] = instance
        return cls._instances[cls][key]
