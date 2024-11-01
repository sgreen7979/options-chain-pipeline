#!/usr/bin/env python3

from enum import IntEnum


class Priority(IntEnum):
    BELOW_NORMAL = 0
    NORMAL = 10
    ABOVE_NORMAL = 20
    URGENT = 30
