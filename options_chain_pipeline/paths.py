#!/usr/bin/env python3
import os

__all__ = [
    "PROJECT_ROOT",
    "MODULE",
    "DATA_PATH",
    "LOG_PATH"
]

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
MODULE = os.path.split(PROJECT_ROOT)[-1]
DATA_PATH = os.path.join(PROJECT_ROOT, "DATA")
if not os.path.exists(DATA_PATH):
    os.makedirs(DATA_PATH, exist_ok=True)
LOG_PATH = os.path.join(PROJECT_ROOT, "logs")
if not os.path.exists(LOG_PATH):
    os.makedirs(LOG_PATH, exist_ok=True)
