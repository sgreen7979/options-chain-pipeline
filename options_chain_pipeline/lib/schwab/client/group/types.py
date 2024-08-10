#!/usr/bin/env python3
from typing import Optional
from typing import TYPE_CHECKING
from typing import TypedDict

if TYPE_CHECKING:
    import datetime as dt
    from ...client import SchwabClient


class ProjectedSchedule(TypedDict):
    run_at: "dt.datetime"
    client: "SchwabClient"
    capacity: int
    next: Optional["ProjectedSchedule"]
