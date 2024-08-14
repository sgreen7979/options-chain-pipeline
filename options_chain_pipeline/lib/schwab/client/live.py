#!/usr/bin/env python3
import json
import time
import threading
from typing import Callable
from typing import Optional
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import redis


class LiveMixin:

    def __init__(
        self,
        callback: Optional[Callable] = None,
        loop=None,
    ):
        self.event = threading.Event()
        self.callback = callback or (lambda _: None)
        self.loop = loop
        self.closing = False

    def set_event(self, event: threading.Event):
        self.event = event

    def set_loop(self, loop):
        self.loop = loop

    def set_callback(self, callback: Callable):
        self.callback = callback

    @property
    def frozen(self):
        return self.event.is_set()

    def handle(self, data, *args, **kwargs):
        return self.callback(data, *args, **kwargs)

    def start(self):
        self.get_logger().info(f"Starting {self.idx}")  # type: ignore
        th = threading.Thread(target=self._consume_queue)
        th.daemon = False
        th.start()

    def stop(self):
        self.closing = True

    def decode_request_dict(self, req_dict) -> dict:
        req_dict_decoded: dict = json.loads(req_dict.decode())
        for k in ("params", "json", "data"):
            if k in req_dict_decoded:
                req_dict_decoded[k] = json.loads(req_dict_decoded[k])
        return req_dict_decoded

    def popqueue(self):
        return self.get_redis().lpop(self.redis_key_req_queue)  # type: ignore

    def _consume_queue(self):
        while not self.closing:
            if req_dict := self.popqueue():  # type: ignore
                req_dict = self.decode_request_dict(req_dict)
                delay = req_dict.pop("delay")
                if delay is not None and delay > 0:
                    time.sleep(delay)
                self._await_capacity()  # type: ignore
                self.handle(self._make_request(**req_dict))  # type: ignore
                while self.frozen:
                    time.sleep(0.1)

        req_dict = self.get_redis().lpop(self.redis_key_req_queue)  # type: ignore
        while req_dict:
            req_dict = self.decode_request_dict(req_dict)
            delay = req_dict.pop("delay")
            if delay is not None and delay > 0:
                time.sleep(delay)
            self._await_capacity()  # type: ignore
            self.handle(self._make_request(**req_dict))  # type: ignore
            while self.frozen:
                time.sleep(0.1)
            req_dict = self.get_redis().lpop(self.redis_key_req_queue)  # type: ignore

    class Queue:

        def __init__(self, redis: "redis.Redis", key):
            self.redis = redis
            self.key = key

        def popright(self):
            return self.redis.rpop(self.key)

        def popleft(self):
            return self.redis.lpop(self.key)

        def insertleft(self, item):
            return self.redis.lpush(self.key, item)

        def insertright(self, item):
            return self.redis.rpush(self.key, item)

        def qsize(self):
            return self.redis.llen(self.key)

        def empty(self):
            return self.qsize() == 0

        def clear(self):
            return self.redis.delete(self.key)

    @property
    def queue(self):
        return self.Queue(self.get_redis(), self.redis_key_req_queue)  # type: ignore
