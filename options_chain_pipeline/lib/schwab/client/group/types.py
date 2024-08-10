from typing import Optional
from typing import TYPE_CHECKING
from typing import TypedDict

if TYPE_CHECKING:
    import datetime as dt
    from daily.schwab.client import SchwabClient


class ProjectedSchedule(TypedDict):
    run_at: "dt.datetime"
    client: "SchwabClient"
    capacity: int
    next: Optional["ProjectedSchedule"]
