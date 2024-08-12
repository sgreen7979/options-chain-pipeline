#!/usr/bin/env python3
from contextlib import contextmanager
import datetime as dt
import json
import threading
import time
from typing import Callable
from typing import ClassVar
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import TYPE_CHECKING

from redis import Redis

from options_chain_pipeline.lib.utils.logging import get_logger
from options_chain_pipeline.lib.utils.singleton import StrictSingleton

from ...client import SchwabClient
from ...credentials.functions import get_all_credentials

if TYPE_CHECKING:
    from ...credentials import SchwabCredentials


__all__ = ["ClientGroup"]
LOG_LEVEL = "INFO"
logger = get_logger(__name__, level=LOG_LEVEL, ch=True, fh=True, fh_level="DEBUG")
redis = Redis()


class ClientGroup(metaclass=StrictSingleton):
    _lock: ClassVar[threading.RLock] = threading.RLock()

    @contextmanager
    @classmethod
    def _acquire_lock(cls):
        if ClientGroup._lock is not None:
            ClientGroup._lock.acquire()
            try:
                yield
            except Exception as e:
                logger.error(e)
            finally:
                ClientGroup._lock.release()

    def __init__(self, clients: Optional[List["SchwabClient"]] = None) -> None:
        self._clients = clients or self._get_clients()

    def _create_client(
        self, credentials: "SchwabCredentials"
    ) -> Optional["SchwabClient"]:
        client = SchwabClient.from_credentials(credentials)
        if not client.login():
            logger.error(f"Failed to login for account index {credentials.idx}")
        else:
            return client

    def _get_clients(self) -> List["SchwabClient"]:
        all_credentials = get_all_credentials()
        clients = [self._create_client(credentials) for credentials in all_credentials]
        clients = [c for c in clients if c is not None]
        if not clients:
            raise RuntimeError("Failed to login with any credentials")
        return clients

    def _get_wait_times(self, incl_queued: bool = False) -> List[float]:
        return [client.get_wait_time(incl_queued) for client in self._clients]

    def _get_next_dts(self, incl_queued: bool = False) -> List[dt.datetime]:
        return [client.get_next_datetime(incl_queued) for client in self._clients]

    def _get_next_ts(self, incl_queued: bool = False) -> List[float]:
        return [client.get_next_timestamp(incl_queued) for client in self._clients]

    def _await_client_with_capacity(self, incl_queued: bool = False) -> "SchwabClient":
        with ClientGroup._lock:
            wait_times = self._get_wait_times(incl_queued)
            min_wait_time = min(wait_times)
            if min_wait_time > 0:
                time.sleep(min_wait_time)
            client = self._clients[wait_times.index(min_wait_time)]
            logger.debug(f"Acquired client {client.idx}")
            return client

    def _get_next_client_with_capacity(self, incl_queued: bool = False):
        """Returns the client with the earliest next capacity time

        NOTE: the client returned doesn't necessarily have capacity currently
        """
        with ClientGroup._lock:
            wait_times = self._get_wait_times(incl_queued)
            min_wait_time = min(wait_times)
            client = self._clients[wait_times.index(min_wait_time)]
            return client

    @property
    def next(self):
        return self._get_next_client_with_capacity(incl_queued=False)

    @contextmanager
    def await_next_client(self, incl_queued: bool = False):
        with ClientGroup._lock:
            wait_times = self._get_wait_times(incl_queued)
            min_wait_time = min(wait_times)
            time.sleep(min_wait_time)
            client = self._clients[wait_times.index(min_wait_time)]
            client._lock.acquire()
            try:
                yield client
            except Exception as e:
                client.get_logger().error(f"Client operation failed {e}", exc_info=True)
            finally:
                client._lock.release()

    def project_schedule(self, incl_queued: bool = False):
        schedule = {}
        next_dts = self._get_next_dts(incl_queued)

        for idx, run_at in enumerate(next_dts):
            schedule[idx] = {"run_at": run_at, "client": self._clients[idx]}

        proj_sched = position = {}
        for i in sorted(schedule, key=lambda i: schedule[i]["run_at"]):
            if proj_sched:
                position["next"] = {}
                position = position["next"]
            position["run_at"] = schedule[i]["run_at"]
            position["client"] = schedule[i]["client"]
            position["capacity"] = schedule[i]["client"].get_capacity_at(
                schedule[i]["run_at"], incl_queued=incl_queued
            )
        return proj_sched

    def _recursive_json_dumps(self, obj):
        if isinstance(obj, (str, int, float, bool)):
            return json.dumps(obj)
        elif isinstance(obj, bytes):
            return json.dumps(obj.decode())
        elif isinstance(obj, (dt.date, dt.datetime)):
            return json.dumps(obj.isoformat())
        elif isinstance(obj, dt.timedelta):
            return json.dumps(obj.total_seconds())
        elif isinstance(obj, Dict):
            return json.dumps(
                {k: self._recursive_json_dumps(v) for k, v in obj.items()}
            )
        elif isinstance(obj, (Tuple, List, Set)):
            return json.dumps(type(obj)(self._recursive_json_dumps(i) for i in obj))
        else:
            return str(obj)

    def schedule_request(
        self,
        service: str,
        params: Dict,
        handler: Callable,
        client: Optional["SchwabClient"] = None,
        run_at: Optional["dt.datetime"] = None,
    ):
        run_at_iso = (dt.datetime.now() if run_at is None else run_at).isoformat()
        req_dict_dumps = json.dumps(
            {"service": service, "params": json.dumps(params), "run_at": run_at_iso}
        )
        if client is None:
            client = self._get_next_client_with_capacity()
        redis_key = client.redis_key_req_queue
        redis.rpush(redis_key, req_dict_dumps)
        client._consume_queue(handler=handler)

    def __len__(self) -> int:
        return len(self._clients)

    def __iter__(self):
        yield from self._clients

    def index(self, client: "SchwabClient") -> int:
        return self._clients.index(client)

    def __getitem__(self, idx: int) -> "SchwabClient":
        return self._clients[idx]

    @property
    def no_current_capacity(self) -> bool:
        with ClientGroup._lock:
            return all(not c.has_capacity(True) for c in self._clients)

    @property
    def capacity_used(self) -> int:
        with ClientGroup._lock:
            return sum(c.capacity_used for c in self._clients)

    @property
    def capacity_queued(self) -> int:
        with ClientGroup._lock:
            return sum(c.capacity_queued for c in self._clients)

    def get_capacity(self, incl_queued: bool = False) -> int:
        with ClientGroup._lock:
            return sum(c.get_capacity(incl_queued) for c in self._clients)

    def has_capacity(self, incl_queued: bool = False) -> bool:
        with ClientGroup._lock:
            return any(c.has_capacity(incl_queued) for c in self._clients)

    def get_capacity_summary(self, incl_queued: bool = False):
        return {
            c.idx: {
                "capacity_used": c.capacity_used,
                "capacity_queued": 0 if not incl_queued else c.capacity_queued,
                "next_availability": c.get_next_datetime(incl_queued).isoformat(),
            }
            for c in self._clients
        }

    @property
    def rolling_capacity(self) -> int:
        return sum(c.rolling_capacity for c in self._clients)
