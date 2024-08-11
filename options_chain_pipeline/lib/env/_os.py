#!/usr/bin/env python3
import platform

OS = platform.system()
IS_WINDOWS = OS == "Windows"
IS_UNIX = OS in ("Darwin", "Linux")
IS_MAC = OS == "Darwin"
IS_LINUX = OS == "Linux"
