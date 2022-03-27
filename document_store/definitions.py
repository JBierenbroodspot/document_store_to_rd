# ----------------------------------------------------------------------------------------------------------------------
#  SPDX-License-Identifier: BSD 3-Clause                                                                               -
#  Copyright (c) 2022 Jimmy Bierenbroodspot.                                                                           -
# ----------------------------------------------------------------------------------------------------------------------
"""A class containing functions, classes and definitions to be used independently by all modules."""
from __future__ import annotations
import typing

T = typing.TypeVar('T')


class Singleton(type):
    """A Metaclass to be used for singleton classes. A singleton is a class that can only be instanced one. This is very
    useful for things like database connections of which you don't want multiple.

    :source: theheadofabroom (20-02-2022). Creating a singleton in Python. Retrieved from Stackoverflow.com:
        https://stackoverflow.com/q/6760685.
    """
    _instances: typing.Dict[typing.Any]

    def __call__(cls: T, *args, **kwargs) -> T:
        if cls not in cls._instances:
            cls._instances = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
