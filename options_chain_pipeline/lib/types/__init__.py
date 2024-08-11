#!/usr/bin/env python3
import os
from pathlib import Path
from typing import List
from typing import Union

__all__ = ["StrPath"]

StrPath = Union[str, os.PathLike[str], Path]
