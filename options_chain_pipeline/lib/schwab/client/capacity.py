#!/usr/bin/env python3
import asyncio
import datetime as dt
import json
import threading
import time
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from redis import Redis

from ..option_chain import OptionChain as OptionsChainParams

redis = Redis()


# class CapacityLimiterMixin(ClientProto):
class CapacityLimiterMixin:


    def get_request_times(self) -> List[float]:
        """Returns a list of timestamps of when requests were made within
        the last 60 seconds
        """
        self._purge_request_times()
        return list(
            sorted([float(t) for t in redis.lrange(self.redis_key_req_times, 0, -1)])
        )

    def has_capacity(self, incl_queued: bool = False) -> bool:
        return self.get_capacity(incl_queued) > 0

    def get_wait_time(self, incl_queued: bool = False) -> float:
        """Returns the number of seconds until client will have capacity"""
        return max(self._get_next_capacity_timestamp(incl_queued) - time.time(), 0.0)

    def get_next_timestamp(self, incl_queued: bool = False) -> float:
        """Returns the timestamp of when the client will next have capacity"""
        return self._get_next_capacity_timestamp(incl_queued)

    def get_next_datetime(self, incl_queued: bool = False) -> "dt.datetime":
        """Returns the datetime of when the client will next have capacity"""
        return dt.datetime.fromtimestamp(self._get_next_capacity_timestamp(incl_queued))

    # FIXME
    def _get_next_capacity_timestamp(
        self,
        incl_queued: bool = False,
    ) -> float:
        """Timestamp at which point capacity will be available"""
        if self.has_capacity(incl_queued):
            return time.time()
        else:
            idx = 0 if not incl_queued else self.capacity_queued - 1
            return self.get_request_times()[idx] + 60.0

    # FIXME
    def get_capacity_at(
        self,
        t: Union[float, "dt.datetime"],
        incl_queued: bool = False,
    ) -> int:
        """Returns the projected capacity at a future time

        :param t: the future time
        :type t: Union[float, dt.datetime]
        :param future_req_times: a list of future request times to include, defaults to None
        :type future_req_times: Optional[List[Union[float, dt.datetime]]], optional
        :return: the projected capacity at time t
        :rtype: int
        """

        if isinstance(t, dt.datetime):
            ts = t.timestamp()
        else:
            ts = t

        request_times = self.get_request_times()
        cutoff = ts - 60.0
        filtered_request_times = [rt for rt in request_times if rt >= cutoff]
        capacity_used = len(filtered_request_times)

        purged = [rt for rt in request_times if rt < cutoff]
        purged_count = len(purged)

        proj_capacity = self.rolling_capacity - capacity_used
        if incl_queued:
            capacity_queued = self.capacity_queued
            for i in range(capacity_queued):
                if (i + 1 > purged_count) or (purged[i] + 60.0 > ts):
                    break
                proj_capacity -= 1
        return proj_capacity

    def _purge_request_times(self) -> int:
        # Use a Lua script to atomically remove old timestamps
        lua_script = """
        local key = KEYS[1]
        local one_minute_ago = tonumber(ARGV[1])
        while true do
            local timestamp = redis.call('lindex', key, 0)
            if not timestamp then break end
            if tonumber(timestamp) >= one_minute_ago then break end
            redis.call('lpop', key)
        end
        return true
        """
        one_minute_ago = time.time() - 60
        return redis.eval(lua_script, 1, self.redis_key_req_times, one_minute_ago)

    def add_request_timestamps(self, timestamps: List[float]):
        for ts in timestamps:
            redis.rpush(self.redis_key_req_times, ts)
        self._purge_request_times()

    def add_request_timestamp(self, ts: Optional[float] = None):
        """Append a timestamp to the list

        :param ts: the timestamp, optional, defaults to the current time
        """
        timestamp = ts or time.time()
        redis.rpush(self.redis_key_req_times, timestamp)
        self._purge_request_times()

    def get_capacity(self, incl_queued: bool = False) -> int:
        capacity_used = self.capacity_used
        capacity_queued = 0 if not incl_queued else self.capacity_queued
        return max(self.rolling_capacity - capacity_used - capacity_queued, 0)

    @property
    def rolling_capacity(self) -> int:
        return self.config["rolling_sixty_limit"]  # type: ignore

    @property
    def capacity_used(self):
        self._purge_request_times()
        return redis.llen(self.redis_key_req_times)

    @property
    def capacity_queued(self) -> int:
        return redis.llen(self.redis_key_req_queue)

    @property
    def redis_key(self) -> str:
        return f"schwab_client:{self.client_id}"  # type: ignore

    @property
    def redis_key_req_times(self) -> str:
        return f"{self.redis_key}:request_times"

    @property
    def redis_key_req_queue(self) -> str:
        return f"{self.redis_key}:request_queue"

    @staticmethod
    def get_redis():
        return redis

    def _await_capacity(self, incl_queued: bool = False):
        wait_time = self.get_wait_time(incl_queued)
        if wait_time > 0:
            time.sleep(wait_time)
        return self
