#!/usr/bin/env python3
import os

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
MODULE = os.path.split(PROJECT_ROOT)[-1]
DATA_PATH = os.path.join(PROJECT_ROOT, "DATA")
if not os.path.exists(DATA_PATH):
    os.mkdir(DATA_PATH)
LOG_PATH = os.path.join(PROJECT_ROOT, "logs")
if not os.path.exists(LOG_PATH):
    os.mkdir(LOG_PATH)
