#!/usr/bin/env python3
from copy import deepcopy
import datetime as dt
import json
import os
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import TYPE_CHECKING
from typing import Union

from options_chain_pipeline.lib.cal import is_bizday
from options_chain_pipeline.lib.cal import get_prior_bizday
from options_chain_pipeline.lib.redis import Client as Redis
from options_chain_pipeline.lib.logging import format_long_list
from options_chain_pipeline.lib.logging import get_logger

if TYPE_CHECKING:
    from options_chain_pipeline.lib.schwab.client import ClientGroup
    from options_chain_pipeline.lib._types import StrPath
    from .models import FundamentalDatabase
    from .models import MetadataDict


REDIS_NAMESPACE = "fundamental"
LOG_LEVEL = "DEBUG"
MAX_LOG_SYMS = 5

# Logging setup
logger = get_logger(
    "options_chain_pipeline.lib.fundamental.loader" if __name__ == "__main__" else __name__,
    level=LOG_LEVEL,
    ch=True,
    ch_level="INFO",
    fh=True,
    fh_level="DEBUG",
)
redis = Redis()


class FundamentalsLoader:

    def __init__(
        self,
        symbols: Union[List[str], Callable[[], List[str]]],
        client_group: "ClientGroup",
        target_date: Optional[dt.date] = None,
        metadata_path: Optional["StrPath"] = None,
        db_path: Optional["StrPath"] = None,
        clear_cache: Optional[bool] = None,
    ) -> None:
        """
        Initialize the FundamentalsLoader with symbols, client group, and optional paths.

        :param symbols: List of symbols or a callable that returns a list of symbols.
        :param client_group: Client group for fetching data.
        :param target_date: Target date for fetching data.
        :param metadata_path: Path to the metadata file.
        :param db_path: Path to the database file.
        :param clear_cache: Whether to clear the Redis cache.
        """
        self.symbols = symbols() if isinstance(symbols, Callable) else symbols
        self.client_group = client_group
        self.target_date = target_date or self._current_target_date()
        from options_chain_pipeline.lib.fundamental import METADATA_PATH
        self.metadata_path = metadata_path or METADATA_PATH
        from options_chain_pipeline.lib.fundamental import FUNDAMENTAL_DB_PATH
        self.db_path = db_path or FUNDAMENTAL_DB_PATH

        self._initialize_db_files()

        if clear_cache is not None and clear_cache:
            self._clear_cache()
        elif clear_cache is None and not self.updated:
            self._clear_cache()

        self._load_existing_dividend_amounts()

        if self.last_updated:
            logger.info(f"last updated: {self.last_updated.date().isoformat()}")

        logger.info(f"updated: {self.updated}")

    @property
    def metadata_exists(self) -> bool:
        return self._exists_with_content(self.metadata_path)

    @property
    def db_exists(self) -> bool:
        return self._exists_with_content(self.db_path)

    @staticmethod
    def _exists_with_content(file_path: "StrPath") -> bool:
        if not os.path.exists(file_path):
            return False
        with open(file_path, "r") as f:
            try:
                json.load(f)
                return True
            except json.JSONDecodeError:
                # no content in file
                return False

    @staticmethod
    def _current_target_date() -> dt.date:
        return dt.date.today() if is_bizday() else get_prior_bizday()

    def _read_metadata(self) -> "MetadataDict":
        logger.debug(f"Reading metadata file {self.metadata_path}")
        with open(self.metadata_path, "r") as f:
            data = json.load(f)
        return data

    def _write_metadata(self, last_updated: Optional[dt.datetime] = None) -> None:
        logger.debug(f"Writing to metadata file {self.metadata_path}")
        metadata = {"last_updated": last_updated.isoformat() if last_updated else None}
        with open(self.metadata_path, "w") as f:
            json.dump(metadata, f)

    def _read_db(self) -> "FundamentalDatabase":
        logger.debug(f"Reading fundamental database file {self.db_path}")
        with open(self.db_path, "r") as f:
            data = json.load(f)
        return data

    def _write_db(self, db: "FundamentalDatabase" = {}) -> None:
        logger.debug(f"Writing to fundamental database file {self.db_path}")
        with open(self.db_path, "w") as f:
            json.dump(db, f)

    def _initialize_db_files(self) -> None:
        if (not self.db_exists) or (not self.metadata_exists):
            logger.info(f"Initializing fundamental database file {self.db_path}")
            self._write_db()
            logger.info(f"Initializing metadata file {self.metadata_path}")
            self._write_metadata()  # set metadata["last_updated"] to None

    @property
    def last_updated(self) -> Optional[dt.datetime]:
        last_updated = self._read_metadata().get("last_updated", None)
        return dt.datetime.fromisoformat(last_updated) if last_updated else None

    @property
    def updated(self) -> bool:
        return (
            self.last_updated is not None
            and self.last_updated.date() >= self.target_date
        )

    @property
    def stale(self) -> bool:
        return not self.updated

    def _load_existing_dividend_amounts(self) -> None:
        if self.db_exists and self.metadata_exists and self.updated:
            logger.info(f"Loading fundamental database {self.db_path}")
            data = self._read_db()
            for symbol, symbol_data in data.items():
                div_amt = symbol_data.get("fundamental", {}).get("divAmount", 0.0)
                self._cache(symbol, div_amt)
                logger.debug(f"Cached {symbol} from existing database")

    @staticmethod
    def _get_cache() -> Dict[str, float]:
        return redis.hgetall(REDIS_NAMESPACE)

    @staticmethod
    def _clear_cache() -> None:
        logger.info(f"Clearing redis cache {REDIS_NAMESPACE}")
        redis.hclr(REDIS_NAMESPACE)
        logger.debug(f"Cleared redis cache {REDIS_NAMESPACE}")

    def _cached(self, symbol: str) -> bool:
        return symbol in self._get_cache()

    @staticmethod
    def _cache(symbol: str, div_amt: float) -> None:
        redis.hset(REDIS_NAMESPACE, symbol, div_amt, exist_ok=True)

    def fetch(self, symbols: Optional[List[str]] = None) -> "FundamentalsLoader":

        if symbols is not None:
            symbols = [s for s in symbols if s not in self.symbols]
            if symbols:
                self.symbols.extend(symbols)
        else:
            symbols = deepcopy(self.symbols)

        logger.debug("Set symbols")

        cached = self._get_cache()
        logger.debug("Collected cache")
        if fetch_syms := [sym for sym in symbols if sym not in cached]:
            sym_str = format_long_list(fetch_syms, MAX_LOG_SYMS)
            logger.info(f"Fetching dividend amounts {sym_str}")
            data = self._read_db() if self.updated else {}
            logger.debug("Set data")
            while fetch_syms:
                client = self.client_group._await_client_with_capacity()
                logger.debug(f"Acquired client {client.idx}")
                capacity_used = min(client.get_capacity(), len(fetch_syms))
                batch = fetch_syms[:capacity_used]
                batch_sym_str = format_long_list(batch, MAX_LOG_SYMS)
                logger.debug(f"Batched symbols {batch_sym_str}")
                batch_data = client.get_quotes(batch, fields=["fundamental"])
                logger.debug(f"Fetched batch data for {len(batch)} symbols")
                for symbol in batch:
                    div_amt = (
                        batch_data.get(symbol, {})
                        .get("fundamental", {})
                        .get("divAmount", 0.0)
                    )
                    self._cache(symbol, div_amt)
                    logger.debug(f"Cached {symbol}")
                data.update(batch_data)
                fetch_syms = fetch_syms[capacity_used:]
            logger.debug("Finished fetching and caching symbols")
            self._write_db(data)
            self._write_metadata(dt.datetime.now())
        return self

    def get(self, symbol: str) -> float:
        if symbol not in self.symbols:
            self.symbols.append(symbol)
            logger.debug(f"Added symbol '{symbol}'")
        if not self._cached(symbol):
            self.fetch([symbol])
        logger.debug(f"Getting {symbol} dividend amount")
        return redis.hget(REDIS_NAMESPACE, symbol, 0.0)
