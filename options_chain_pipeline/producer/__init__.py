#!/usr/bin/env python3
import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import datetime as dt
import gzip
import json
import sys
import time
from typing import Any
from typing import Callable
from typing import Coroutine
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import TYPE_CHECKING
from typing import Union

from options_chain_pipeline.lib.fundamental.loader import FundamentalsLoader
from options_chain_pipeline.lib.market_hours import functions as mh
from options_chain_pipeline.lib.schwab.exceptions import ReadTimeoutError
from options_chain_pipeline.lib.schwab.exceptions import UnexpectedTokenAuthError
from options_chain_pipeline.lib.schwab.option_chain import OptionChain as OptionsChainParams
from options_chain_pipeline.lib.symbols import get_options_universe
from options_chain_pipeline.lib.utils.logging import get_logger

from .base_producer import BaseKafkaProducer

if TYPE_CHECKING:
    from daily.schwab.client import SchwabClient
    from logging import Logger

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"
KAFKA_TOPIC = "option_chain_topic"
KAFKA_NUM_PARTITIONS = 5
KAFKA_REPLICATION_FACTOR = 1
KAFKA_CONFIG = {
    "topic_prefix": "option_chain_topic",
    "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
    "num_partitions": KAFKA_NUM_PARTITIONS,
    "replication_factor": KAFKA_REPLICATION_FACTOR,
    "max_request_size": 10_485_760,  # 10MB
    "compression_type": 'gzip',
    "value_serializer": lambda v: gzip.compress(json.dumps(v).encode('utf-8')),
}
MAX_WORKERS_EXECUTOR = 20

# Logging setup
LOG_LEVEL = "DEBUG"
logger = get_logger(
    "daily.schwab.options_producer" if __name__ == "__main__" else __name__,
    level=LOG_LEVEL,
    ch=True,
    ch_level="INFO",
    fh=True,
    fh_level="DEBUG",
    fh_fmt="%(asctime)s %(levelname)s %(name)s %(pathname)s %(lineno)d %(message)s",
    fh_type="RotatingFileHandler",
    fh_type_kwargs={"maxBytes": 1_048_576, "backupCount": 500_000},  # 1MB
)


class OptionsChainProducer(BaseKafkaProducer):
    def __init__(
        self,
        chain_req_params: "OptionsChainParams",
        symbols: Union[List[str], Callable[[], List[str]]],
        *,
        test: bool = False,
        test_time_minutes: float = 20.0,
        sink: bool = True,
        kafka_config: Optional[Dict] = None,
        fundamentals_loader: Optional["FundamentalsLoader"] = None,
        regstart: Optional[dt.datetime] = None,
        regend: Optional[dt.datetime] = None,
    ) -> None:
        self._kafka_config = kafka_config or deepcopy(KAFKA_CONFIG)
        super().__init__(
            symbols,
            topic_prefix=self._kafka_config["topic_prefix"],
            bootstrap_servers=self._kafka_config["bootstrap_servers"],
            num_partitions=self._kafka_config.get("num_partitions", 1),
            replication_factor=self._kafka_config.get("replication_factor", 1),
            compression_type=self._kafka_config.get("compression_type"),
            value_serializer=self._kafka_config.get("value_serializer"),
            max_request_size=self._kafka_config.get("max_request_size", 1_048_576),
        )

        self.request_params = chain_req_params.query_parameters
        self._sink = sink

        if fundamentals_loader is not None:
            self.fundamentals = fundamentals_loader.fetch(self._symbols)
        else:
            self.fundamentals = FundamentalsLoader(self._symbols, self.clients).fetch()

        # For testing purposes, it is safer getting and setting our hours after
        # the upfront work in fetching dividendAmounts
        self.regstart, self.regend = self._get_hours(
            regstart, regend, test, test_time_minutes
        )

        logger.info(f"regstart: {self.regstart.isoformat()}")
        logger.info(f"regend: {self.regend.isoformat()}")
        logger.info("Initialized OptionsChainProducer")

    def get_logger(self) -> "Logger":
        return logger

    @property
    def expected_request_count(self) -> int:
        return int(
            (self.regend - self.regstart).total_seconds()
            / 60
            * self.clients.rolling_capacity
        )

    def cleanup(self):
        self.fundamentals._clear_cache()
        super().cleanup()
      
    def add_symbol(self, symbol: str) -> None:
        super().add_symbol(symbol)
        self.fundamentals.fetch([symbol])

    def add_symbols(self, symbols: List[str]) -> None:
        super().add_symbols(symbols)
        self.fundamentals.fetch(symbols)

    def _get_hours(
        self,
        regstart: Optional[dt.datetime],
        regend: Optional[dt.datetime],
        test: bool,
        test_time_minutes: float,
    ) -> Tuple[dt.datetime, dt.datetime]:
        if regstart is not None and regend is not None:
            assert (
                regend > regstart
            ), f"regstart >= regend (regstart={regstart.isoformat()}, regend={regend.isoformat()})"
            assert (regend - regstart).total_seconds() <= (
                6.5 * 3600
            ), f"regend - regstart > 6.5 hours (timedelta={(regend - regstart).total_seconds() / 3600}hrs)"
            return regstart, regend
        elif test:
            regstart = dt.datetime.now()
            regend = regstart + dt.timedelta(minutes=test_time_minutes)
            logger.info(f"Testing for {test_time_minutes} minutes")
            return regstart, regend
        else:
            hours = mh.fetch_today()

            if not mh.isOpen(hours, "option"):
                logger.error("Options markets are closed.")
                self.cleanup()
                sys.exit(-1)

            return (
                mh.get_hour(hours, "$.option.EQO.sessionHours.regularMarket[0].start"),
                mh.get_hour(hours, "$.option.IND.sessionHours.regularMarket[0].end"),
            )

    def _validate_chain_response_data(self, resp_data: Dict) -> bool:
        return (resp_data or {}).get("status") == "SUCCESS"

    def get_options_chain(self, client: "SchwabClient", symbol: str) -> Dict:
        params = self._get_params(symbol)
        try:
            return client.get_options_chain(params)
        except UnexpectedTokenAuthError as e:
            client.get_logger().error(e)
            logger.error(str(e))
            run_at = dt.datetime.now() + dt.timedelta(seconds=15)
            self.clients.schedule_request(
                "options_chain", params, self._handle_data, run_at=run_at
            )
            return {
                "status": "FAILURE",
                "error": e,
                "fetchTime": e.fetch_time,
                "response": e.response,
                "symbol": symbol,
                "client": client,
            }
        except ReadTimeoutError as e:
            client.get_logger().error(e)
            logger.error(str(e))
            run_at = dt.datetime.now() + dt.timedelta(seconds=15)
            self.clients.schedule_request(
                "options_chain", params, self._handle_data, run_at=run_at
            )
            return {
                "status": "FAILURE",
                "error": e.message,
                "fetchTime": e.fetch_time,
                "prepared_request": e.prepared_request,
                "symbol": symbol,
                "client": client,
            }
        except Exception as e:
            client.get_logger().error(e)
            logger.error(str(e))
            return {
                "status": "FAILURE",
                "error": e,
                "fetchTime": dt.datetime.now(),
                "symbol": symbol,
                "client": client,
            }

    def _handle_data(self, data: Dict, idx: int, metadata: Optional[Dict] = None):
        symbol = data.get("symbol", "")
        if "error" in data:
            logger.error(data["error"])

        if self._validate_chain_response_data(data):
            data["dividendAmount"] = self.fundamentals.get(symbol)
            if "response" in data:
                del data["response"]
            self._to_kafka(symbol, data)
        else:
            logger.error(f"Invalid request data {symbol}")
            if self._running and self._round_trip <= 1 and symbol != "GME":
                self.remove_symbol(symbol)

    async def async_fetch(
        self,
        client: "SchwabClient",
        symbol: str,
        executor: "ThreadPoolExecutor",
        metadata: Optional[Dict] = None,
    ) -> Optional[Dict]:
        loop = asyncio.get_running_loop()

        logger.debug(f"Fetching option chain data {symbol} (client_idx={client.idx})")
        data = await loop.run_in_executor(
            executor, self.get_options_chain, client, symbol
        )

        if self._validate_chain_response_data(data):
            data["dividendAmount"] = self.fundamentals.get(symbol)
            if "response" in data:
                del data["response"]
            loop.call_soon(self._to_kafka, symbol, data)
            # self._to_kafka(symbol, data)
        else:
            logger.error(f"Invalid request data {symbol}")
            if self._running and self._round_trip <= 1 and symbol != "GME":
                self.remove_symbol(symbol)

    def _get_params(self, symbol: str) -> Dict[str, Any]:
        params = deepcopy(self.request_params)
        params.update({"symbol": symbol})
        return params

    async def _submit(self, tasks: List[Coroutine]):
        await asyncio.gather(*tasks)

    async def _in_executor(
        self,
        client: "SchwabClient",
        symbols: List[str],
        meth: Optional[Callable] = None,
    ):
        meth = meth or self.async_fetch
        max_workers = MAX_WORKERS_EXECUTOR
        with ThreadPoolExecutor(max_workers) as executor:
            tasks = [meth(client, symbol, executor) for symbol in symbols]
            await self._submit(tasks)

    async def run(self) -> None:
        self._ran = True

        now = dt.datetime.now()
        if now > self.regend:
            logger.info("Options markets are closed.")
            self.cleanup()
            return

        self._running = True
        if now < self.regstart:
            await asyncio.sleep(max((self.regstart - now).total_seconds(), 0.0))

        logger.info("Running OptionsChainProducer")
        while dt.datetime.now() < self.regend:
            if not self._symbols:
                logger.error("self._symbols is empty")
                await asyncio.sleep(30)
                continue

            self._round_trip += 1
            self._batch_num = 0

            if (
                dt.datetime.now() + dt.timedelta(minutes=self.round_trip_time)
                > self.regend
            ):
                logger.info(f"Starting FINAL round trip {self._round_trip}")
            else:
                logger.info(f"Starting round trip {self._round_trip}")

            # symbols = deepcopy(self._symbols)
            symbols = self.symbols
            start_time = time.perf_counter()
            while symbols:
                client = self.clients._await_client_with_capacity(incl_queued=True)
                capacity_used = min(client.get_capacity(incl_queued=True), len(symbols))
                batch = symbols[:capacity_used]
                self._batch_num += 1
                await self._in_executor(client, batch)
                symbols = symbols[capacity_used:]
            end_time = time.perf_counter()
            elapsed = int((end_time - start_time) // 60.0)
            logger.info(
                f"Completed round trip {self._round_trip} in ~{elapsed} minutes"
            )
            await asyncio.sleep(0.25)
        self._running = False
        logger.info("Options markets are closed.")
        self.cleanup()


def parse_hours(date_string):
    try:
        return dt.datetime.fromisoformat(date_string)
    except:
        return dt.datetime.fromtimestamp(float(date_string))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", dest="test", action="store_true", default=False)
    parser.add_argument("--test-time", dest="test_time", type=float, default=20.0)
    parser.add_argument("--no-sink", dest="sink", action="store_false", default=True)
    parser.add_argument("--regstart", dest="regstart", type=parse_hours, default=None)
    parser.add_argument("--regend", dest="regend", type=parse_hours, default=None)
    return parser.parse_args()


def main():
    args = parse_args()
    chain_request_params = OptionsChainParams(
        symbol="<symbol>",
        include_quotes=True,
        strategy=OptionsChainParams.Strategy.ANALYTICAL,
    )
    producer = OptionsChainProducer(
        chain_request_params,
        get_options_universe,
        test=args.test,
        test_time_minutes=args.test_time,
        sink=args.sink,
        regstart=args.regstart,
        regend=args.regend,
    )
    asyncio.run(producer.run())


if __name__ == "__main__":
    main()
