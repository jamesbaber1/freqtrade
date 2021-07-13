""" Alpaca exchange subclass """
import asyncio
import http
import inspect
import logging
import os
from copy import deepcopy
from datetime import datetime, timedelta
from math import ceil
from typing import Any, Dict, List, Optional, Tuple

import arrow
import ccxt
import ccxt.async_support as ccxt_async
from cachetools import TTLCache
from ccxt.base.decimal_to_precision import (ROUND_DOWN, ROUND_UP, TICK_SIZE, TRUNCATE,
                                            decimal_to_precision)
from pandas import DataFrame

from freqtrade.constants import DEFAULT_AMOUNT_RESERVE_PERCENT, ListPairsWithTimeframes
from freqtrade.data.converter import ohlcv_to_dataframe, trades_dict_to_list
from freqtrade.exceptions import (DDosProtection, ExchangeError, InsufficientFundsError,
                                  InvalidOrderException, OperationalException, PricingError,
                                  RetryableOrderError, TemporaryError)
from freqtrade.exchange.common import (API_FETCH_ORDER_RETRY_COUNT, BAD_EXCHANGES,
                                       EXCHANGE_HAS_OPTIONAL, EXCHANGE_HAS_REQUIRED, retrier,
                                       retrier_async)
from freqtrade.misc import deep_merge_dicts, safe_value_fallback2
from freqtrade.plugins.pairlist.pairlist_helpers import expand_pairlist

CcxtModuleType = Any



alpaca_trade_api_logger = logging.getLogger('alpaca_trade_api.rest')
alpaca_trade_api_logger.setLevel(level=logging.ERROR)


logger = logging.getLogger(__name__)

from freqtrade import exchange
from alpaca_trade_api.rest import REST, TimeFrame, BarsV2
from pprint import pprint


class Alpaca(exchange.Exchange):

    def __init__(self, config: Dict[str, Any], validate: bool = True) -> None:
        """
        Initializes this module with the given config,
        it does basic validation whether the specified exchange and pairs are valid.
        :return: None
        """
        self._api = AlpacaApi(
            key_id=config['exchange']['key'],
            secret_key=config['exchange']['secret']
        )
        self._api_async = AlpacaAsyncApi(
            key_id=config['exchange']['key'],
            secret_key=config['exchange']['secret']
        )

        self._markets: Dict = {}

        self._config.update(config)

        # Holds last candle refreshed time of each pair
        self._pairs_last_refresh_time: Dict[Tuple[str, str], int] = {}
        # Timestamp of last markets refresh
        self._last_markets_refresh: int = 0

        # Cache for 10 minutes ...
        self._fetch_tickers_cache: TTLCache = TTLCache(maxsize=1, ttl=60 * 10)
        # Cache values for 1800 to avoid frequent polling of the exchange for prices
        # Caching only applies to RPC methods, so prices for open trades are still
        # refreshed once every iteration.
        self._sell_rate_cache: TTLCache = TTLCache(maxsize=100, ttl=1800)
        self._buy_rate_cache: TTLCache = TTLCache(maxsize=100, ttl=1800)

        # Holds candles
        self._klines: Dict[Tuple[str, str], DataFrame] = {}

        # Holds all open sell orders for dry_run
        self._dry_run_open_orders: Dict[str, Any] = {}

        if config['dry_run']:
            logger.info('Instance is running with dry_run enabled')

        exchange_config = config['exchange']
        self.log_responses = exchange_config.get('log_responses', False)

        # Deep merge ft_has with default ft_has options
        self._ft_has = deep_merge_dicts(self._ft_has, deepcopy(self._ft_has_default))
        if exchange_config.get('_ft_has_params'):
            self._ft_has = deep_merge_dicts(exchange_config.get('_ft_has_params'),
                                            self._ft_has)
            logger.info("Overriding exchange._ft_has with config params, result: %s", self._ft_has)

        # Assign this directly for easy access
        self._ohlcv_partial_candle = self._ft_has['ohlcv_partial_candle']

        self._trades_pagination = self._ft_has['trades_pagination']
        self._trades_pagination_arg = self._ft_has['trades_pagination_arg']
        #
        # # # Initialize ccxt objects
        # # ccxt_config = self._ccxt_config.copy()
        # # ccxt_config = deep_merge_dicts(exchange_config.get('ccxt_config', {}), ccxt_config)
        # # ccxt_config = deep_merge_dicts(exchange_config.get('ccxt_sync_config', {}), ccxt_config)
        # #
        # # self._api = self._init_ccxt(exchange_config, ccxt_kwargs=ccxt_config)
        # #
        # # ccxt_async_config = self._ccxt_config.copy()
        # # ccxt_async_config = deep_merge_dicts(exchange_config.get('ccxt_config', {}),
        # #                                      ccxt_async_config)
        # # ccxt_async_config = deep_merge_dicts(exchange_config.get('ccxt_async_config', {}),
        # #                                      ccxt_async_config)
        # # self._api_async = self._init_ccxt(
        # #     exchange_config, ccxt_async, ccxt_kwargs=ccxt_async_config)
        # #
        # # logger.info('Using Exchange "%s"', self.name)
        #
        # if validate:
        #     # Check if timeframe is available
        #     self.validate_timeframes(config.get('timeframe'))
        #
        #     # Initial markets load
        #     self._load_markets()
        #
        #     # Check if all pairs are available
        #     self.validate_stakecurrency(config['stake_currency'])
        #     if not exchange_config.get('skip_pair_validation'):
        #         self.validate_pairs(config['exchange']['pair_whitelist'])
        #     self.validate_ordertypes(config.get('order_types', {}))
        #     self.validate_order_time_in_force(config.get('order_time_in_force', {}))
        #     self.validate_required_startup_candles(config.get('startup_candle_count', 0),
        #                                            config.get('timeframe', ''))
        #
        # # Converts the interval provided in minutes in config to seconds
        # self.markets_refresh_interval: int = exchange_config.get(
        #     "markets_refresh_interval", 60) * 60

    def __del__(self):
        """
        Destructor - clean up async stuff
        """
        pass
        # self.close()

    def validate_pairs(self, pairs: List[str]) -> None:
        """
        Checks if all given pairs are tradable on the current exchange.
        :param pairs: list of pairs
        :raise: OperationalException if one pair is not available
        :return: None
        """
        pass

    def validate_timeframes(self, timeframe: Optional[str]) -> None:
        """
        Check if timeframe from config is a supported timeframe on the exchange
        """
        pass
        # if not hasattr(self._api, "timeframes") or self._api.timeframes is None:
        #     # If timeframes attribute is missing (or is None), the exchange probably
        #     # has no fetchOHLCV method.
        #     # Therefore we also show that.
        #     raise OperationalException(
        #         f"The ccxt library does not provide the list of timeframes "
        #         f"for the exchange \"{self.name}\" and this exchange "
        #         f"is therefore not supported. ccxt fetchOHLCV: {self.exchange_has('fetchOHLCV')}")
        #
        # if timeframe and (timeframe not in self.timeframes):
        #     raise OperationalException(
        #         f"Invalid timeframe '{timeframe}'. This exchange supports: {self.timeframes}")
        #
        # if timeframe and timeframe_to_minutes(timeframe) < 1:
        #     raise OperationalException("Timeframes < 1m are currently not supported by Freqtrade.")

        # if not self.markets:
        #     logger.warning('Unable to validate pairs (assuming they are correct).')
        #     return
        # extended_pairs = expand_pairlist(pairs, list(self.markets), keep_invalid=True)
        # invalid_pairs = []
        # for pair in extended_pairs:
        #     # Note: ccxt has BaseCurrency/QuoteCurrency format for pairs
        #     if self.markets and pair not in self.markets:
        #         raise OperationalException(
        #             f'Pair {pair} is not available on {self.name}. '
        #             f'Please remove {pair} from your whitelist.')
        #
        #         # From ccxt Documentation:
        #         # markets.info: An associative array of non-common market properties,
        #         # including fees, rates, limits and other general market information.
        #         # The internal info array is different for each particular market,
        #         # its contents depend on the exchange.
        #         # It can also be a string or similar ... so we need to verify that first.
        #     elif (isinstance(self.markets[pair].get('info', None), dict)
        #           and self.markets[pair].get('info', {}).get('IsRestricted', False)):
        #         # Warn users about restricted pairs in whitelist.
        #         # We cannot determine reliably if Users are affected.
        #         logger.warning(f"Pair {pair} is restricted for some users on this exchange."
        #                        f"Please check if you are impacted by this restriction "
        #                        f"on the exchange and eventually remove {pair} from your whitelist.")
        #     if (self._config['stake_currency'] and
        #             self.get_pair_quote_currency(pair) != self._config['stake_currency']):
        #         invalid_pairs.append(pair)
        # if invalid_pairs:
        #     raise OperationalException(
        #         f"Stake-currency '{self._config['stake_currency']}' not compatible with "
        #         f"pair-whitelist. Please remove the following pairs: {invalid_pairs}")

    def market_is_tradable(self, market: Dict[str, Any]) -> bool:
        """
        Check if the market symbol is tradable by Freqtrade.
        By default, checks if it's splittable by `/` and both sides correspond to base / quote
        """
        return True

    def get_pair_quote_currency(self, pair: str) -> str:
        """
        Return a pair's quote currency
        """
        return 'USD'


class AlpacaApi(REST):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = 'alpaca'
        self.id = 'alpaca-wrapper'
        self.timeframes = {
            '1d': TimeFrame.Day,
            '1h': TimeFrame.Hour,
            '1m': TimeFrame.Minute
        }

    @property
    def markets(self):
        return list(self.load_markets().keys())

    @staticmethod
    def calculate_fee(*args, **kwargs):
        return {'rate': 0}

    @staticmethod
    def get_start(since):
        start = datetime.fromtimestamp(since / 1000.0)
        start = datetime(
            year=start.year,
            month=start.month,
            day=start.day,
            hour=start.hour,
            minute=start.minute,
        )
        return f'{start.isoformat()}Z'

    @staticmethod
    def get_end():
        now = datetime.utcnow()
        now = datetime(
            year=now.year,
            month=now.month,
            day=now.day,
            hour=now.hour,
            minute=now.minute,
        )
        return f'{(now - timedelta(minutes=16)).isoformat()}Z'

    @staticmethod
    def format_bars(bars):
        return [[int(bar.t.timestamp()) * 1000, bar.o, bar.h, bar.l, bar.c, bar.v] for bar in bars]

    def load_markets(self, *args, **kwargs):
        return {asset.symbol: asset._raw for asset in self.list_assets()}

    def fetch_ohlcv(self, symbol, timeframe='1m', since=None, limit=None, params={}):
        bars = self.get_bars(
            symbol=symbol,
            timeframe=self.timeframes[timeframe],
            start=self.get_start(since),
            end=self.get_end(),
            # limit=self.ohlcv_candle_limit(timeframe)
        )
        return self.format_bars(bars)


class AlpacaAsyncApi(AlpacaApi):
    async def load_markets(self, *args, **kwargs):
        return super().load_markets(*args, **kwargs)

    async def fetch_ohlcv(self, symbol, timeframe='1m', since=None, limit=None, params={}):
        bars = self.get_bars(
            symbol=symbol,
            timeframe=self.timeframes[timeframe],
            start=self.get_start(since),
            end=self.get_end(),
            # limit=self.ohlcv_candle_limit(timeframe)
        )
        return self.format_bars(bars)