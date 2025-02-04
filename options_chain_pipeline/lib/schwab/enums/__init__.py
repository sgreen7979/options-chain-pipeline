# Adapted from https://github.com/areed1192/td-ameritrade-python-api by Alex Reed

# todo: reconcile any Schwab changes
from enum import Enum


'''
    ORDERS ENUMS
'''


class ORDER_SESSION(Enum):

    NORMAL = 'NORMAL'
    AM = 'AM'
    PM = 'PM'
    SEAMLESS = 'SEAMLESS'


class DURATION(Enum):
    """DAY, GOOD_TILL_CANCEL, FILL_OR_KILL, IMMEDIATE_OR_CANCEL, END_OF_WEEK, END_OF_MONTH, NEXT_END_OF_MONTH, UNKNOWN"""

    DAY = 'DAY'
    GOOD_TILL_CANCEL = 'GOOD_TILL_CANCEL'
    FILL_OR_KILL = 'FILL_OR_KILL'


class ORDER_STRATEGY_TYPE(Enum):
    """SINGLE, CANCEL, RECALL, PAIR, FLATTEN, TWO_DAY_SWAP, BLAST_ALL, OCO, TRIGGER"""

    SINGLE = 'SINGLE'
    OCO = 'OCO'
    TRIGGER = 'TRIGGER'


class QUANTITY_TYPE(Enum):

    ALL_SHARES = 'ALL_SHARES'
    DOLLARS = 'DOLLARS'
    SHARES = 'SHARES'


class ORDER_ASSET_TYPE(Enum):
    """EQUITY, OPTION, INDEX, MUTUAL_FUND, CASH_EQUIVALENT, FIXED_INCOME, CURRENCY, COLLECTIVE_INVESTMENT"""

    EQUITY = 'EQUITY'
    OPTION = 'OPTION'
    INDEX = 'INDEX'
    MUTUAL_FUND = 'MUTUAL_FUND'
    CASH_EQUIVALENT = 'CASH_EQUIVALENT'
    FIXED_INCOME = 'FIXED_INCOME'
    CURRENCY = 'CURRENCY'


class COMPLEX_ORDER_STRATEGY_TYPE(Enum):
    """NONE, COVERED, VERTICAL, BACK_RATIO, CALENDAR, DIAGONAL, STRADDLE,
    STRANGLE, COLLAR_SYNTHETIC, BUTTERFLY, CONDOR, IRON_CONDOR, VERTICAL_ROLL,
    COLLAR_WITH_STOCK, DOUBLE_DIAGONAL, UNBALANCED_BUTTERFLY, UNBALANCED_CONDOR,
    UNBALANCED_IRON_CONDOR, UNBALANCED_VERTICAL_ROLL, MUTUAL_FUND_SWAP, CUSTOM"""

    NONE = 'NONE'
    COVERED = 'COVERED'
    VERTICAL = 'VERTICAL'
    BACK_RATIO = 'BACK_RATIO'
    CALENDAR = 'CALENDAR'
    DIAGONAL = 'DIAGONAL'
    STRADDLE = 'STRADDLE'
    STRANGLE = 'STRANGLE'
    COLLAR_SYNTHETIC = 'COLLAR_SYNTHETIC'
    BUTTERFLY = 'BUTTERFLY'
    CONDOR = 'CONDOR'
    IRON_CONDOR = 'IRON_CONDOR'
    VERTICAL_ROLL = 'VERTICAL_ROLL'
    COLLAR_WITH_STOCK = 'COLLAR_WITH_STOCK'
    DOUBLE_DIAGONAL = 'DOUBLE_DIAGONAL'
    UNBALANCED_BUTTERFLY = 'UNBALANCED_BUTTERFLY'
    UNBALANCED_CONDOR = 'UNBALANCED_CONDOR'
    UNBALANCED_IRON_CONDOR = 'UNBALANCED_IRON_CONDOR'
    UNBALANCED_VERTICAL_ROLL = 'UNBALANCED_VERTICAL_ROLL'
    CUSTOM = 'CUSTOM'


class ORDER_INSTRUCTIONS(Enum):
    """BUY, SELL, BUY_TO_COVER, SELL_SHORT, BUY_TO_OPEN, BUY_TO_CLOSE,
    SELL_TO_OPEN, SELL_TO_CLOSE, EXCHANGE, SELL_SHORT_EXEMPT"""

    BUY = 'BUY'
    SELL = 'SELL'
    BUY_TO_COVER = 'BUY_TO_COVER'
    SELL_SHORT = 'SELL_SHORT'
    BUY_TO_OPEN = 'BUY_TO_OPEN'
    BUY_TO_CLOSE = 'BUY_TO_CLOSE'
    SELL_TO_OPEN = 'SELL_TO_OPEN'
    SELL_TO_CLOSE = 'SELL_TO_CLOSE'
    EXCHANGE = 'EXCHANGE'


class REQUESTED_DESTINATION(Enum):

    INET = 'INET'
    ECN_ARCA = 'ECN_ARCA'
    CBOE = 'CBOE'
    AMEX = 'AMEX'
    PHLX = 'PHLX'
    ISE = 'ISE'
    BOX = 'BOX'
    NYSE = 'NYSE'
    NASDAQ = 'NASDAQ'
    BATS = 'BATS'
    C2 = 'C2'
    AUTO = 'AUTO'


class STOP_PRICE_LINK_BASIS(Enum):
    """MANUAL, BASE, TRIGGER, LAST, BID, ASK, ASK_BID, MARK, AVERAGE"""

    MANUAL = 'MANUAL'
    BASE = 'BASE'
    TRIGGER = 'TRIGGER'
    LAST = 'LAST'
    BID = 'BID'
    ASK = 'ASK'
    ASK_BID = 'ASK_BID'
    MARK = 'MARK'
    AVERAGE = 'AVERAGE'


class STOP_PRICE_LINK_TYPE(Enum):

    VALUE = 'VALUE'
    PERCENT = 'PERCENT'
    TICK = 'TICK'


class STOP_TYPE(Enum):
    """STANDARD, BID, ASK, LAST, MARK"""

    STANDARD = 'STANDARD'
    BID = 'BID'
    ASK = 'ASK'
    LAST = 'LAST'
    MARK = 'MARK'


class PRICE_LINK_BASIS(Enum):
    """MANUAL, BASE, TRIGGER, LAST, BID, ASK, ASK_BID, MARK, AVERAGE"""

    MANUAL = 'MANUAL'
    BASE = 'BASE'
    TRIGGER = 'TRIGGER'
    LAST = 'LAST'
    BID = 'BID'
    ASK = 'ASK'
    ASK_BID = 'ASK_BID'
    MARK = 'MARK'
    AVERAGE = 'AVERAGE'


class PRICE_LINK_TYPE(Enum):
    """"""

    VALUE = 'VALUE'
    PERCENT = 'PERCENT'
    TICK = 'TICK'


class ORDER_TYPE(Enum):
    """DAY, GOOD_TILL_CANCEL, FILL_OR_KILL, IMMEDIATE_OR_CANCEL, END_OF_WEEK, END_OF_MONTH, NEXT_END_OF_MONTH, UNKNOWN"""

    MARKET = 'MARKET'
    LIMIT = 'LIMIT'
    STOP = 'STOP'
    STOP_LIMIT = 'STOP_LIMIT'
    TRAILING_STOP = 'TRAILING_STOP'
    MARKET_ON_CLOSE = 'MARKET_ON_CLOSE'
    EXERCISE = 'EXERCISE'
    TRAILING_STOP_LIMIT = 'TRAILING_STOP_LIMIT'
    NET_DEBIT = 'NET_DEBIT'
    NET_CREDIT = 'NET_CREDIT'
    NET_ZERO = 'NET_ZERO'


class POSITION_EFFECT(Enum):

    OPENING = 'OPENING'
    CLOSING = 'CLOSING'
    AUTOMATIC = 'AUTOMATIC'


class TAX_LOT_METHOD(Enum):
    """FIFO, LIFO, HIGH_COST, LOW_COST, AVERAGE_COST, SPECIFIC_LOT, LOSS_HARVESTER"""

    FIFO = 'FIFO'
    LIFO = 'LIFO'
    HIGH_COST = 'HIGH_COST'
    LOW_COST = 'LOW_COST'
    AVERAGE_COST = 'AVERAGE_COST'
    SPECIFIC_LOT = 'SPECIFIC_LOT'


class SPECIAL_INSTRUCTIONS(Enum):
    """ALL_OR_NONE, DO_NOT_REDUCE, ALL_OR_NONE_DO_NOT_REDUCE"""

    ALL_OR_NONE = 'ALL_OR_NONE'
    DO_NOT_REDUCE = 'DO_NOT_REDUCE'
    ALL_OR_NONE_DO_NOT_REDUCE = 'ALL_OR_NONE_DO_NOT_REDUCE'


class STATUS(Enum):
    AWAITING_RELEASE_TIME = 'AWAITING_RELEASE_TIME'
    NEW = 'NEW'
    PENDING_RECALL = 'PENDING_RECALL'
    PENDING_ACKNOWLEDGEMENT = 'PENDING_ACKNOWLEDGEMENT'
    AWAITING_STOP_CONDITION = 'AWAITING_STOP_CONDITION'
    AWAITING_PARENT_ORDER = 'AWAITING_PARENT_ORDER'
    AWAITING_CONDITION = 'AWAITING_CONDITION'
    AWAITING_MANUAL_REVIEW = 'AWAITING_MANUAL_REVIEW'
    ACCEPTED = 'ACCEPTED'
    AWAITING_UR_OUT = 'AWAITING_UR_OUT'
    PENDING_ACTIVATION = 'PENDING_ACTIVATION'
    QUEUED = 'QUEUED'
    WORKING = 'WORKING'
    REJECTED = 'REJECTED'
    PENDING_CANCEL = 'PENDING_CANCEL'
    CANCELED = 'CANCELED'
    PENDING_REPLACE = 'PENDING_REPLACE'
    REPLACED = 'REPLACED'
    FILLED = 'FILLED'
    EXPIRED = 'EXPIRED'
    UNKNOWN = 'UNKNOWN'


class MUTUAL_FUND_TYPES(Enum):

    NOT_APPLICABLE = 'NOT_APPLICABLE'
    OPEN_END_NON_TAXABLE = 'OPEN_END_NON_TAXABLE'
    OPEN_END_TAXABLE = 'OPEN_END_TAXABLE'
    NO_LOAD_NON_TAXABLE = 'NO_LOAD_NON_TAXABLE'
    NO_LOAD_TAXABLE = 'NO_LOAD_TAXABLE'


class CASH_EQUIVALENT_TYPE(Enum):

    SAVINGS = 'SAVINGS'
    MONEY_MARKET_FUND = 'MONEY_MARKET_FUND'


class OPTION_TYPE(Enum):

    VANILLA = 'VANILLA'
    BINARY = 'BINARY'
    BARRIER = 'BARRIER'


class PUT_CALL(Enum):

    PUT = 'PUT'
    CALL = 'CALL'


class CURRENCY_TYPE(Enum):

    USD = 'USD'
    CAD = 'CAD'
    EUR = 'EUR'
    JPY = 'JPY'


'''
    OPTION CHAIN ENUMS
'''


class OPTION_CHAIN_STRATEGY(Enum):

    SINGLE = 'SINGLE'
    ANALYTICAL = 'ANALYTICAL'
    COVERED = 'COVERED'
    VERTICAL = 'VERTICAL'
    CALENDAR = 'CALENDAR'
    STRANGLE = 'STRANGLE'
    STRADDLE = 'STRADDLE'
    BUTTERFLY = 'BUTTERFLY'
    CONDOR = 'CONDOR'
    DIAGONAL = 'DIAGONAL'
    COLLAR = 'COLLAR'
    ROLL = 'ROLL'


class OPTION_CHAIN_RANGE(Enum):

    ITM = 'ITM'
    NTM = 'NTM'
    OTM = 'OTM'
    SAK = 'SAK'
    SBK = 'SBK'
    SNK = 'SNK'
    ALL = 'ALL'


class OPTION_CHAIN_EXP_MONTH(Enum):

    ALL = 'ALL'
    JAN = 'JAN'
    FEB = 'FEB'
    MAR = 'MAR'
    APR = 'APR'
    MAY = 'MAY'
    JUN = 'JUN'
    JUL = 'JUL'
    AUG = 'AUG'
    SEP = 'SEP'
    OCT = 'OCT'
    DEC = 'DEC'


class OPTION_CHAIN_OPTION_TYPE(Enum):

    S = 'S'
    NS = 'NS'
    ALL = 'ALL'


class STREAM_ACTIVES(Enum):
    pass


ENDPOINT_ARGUMENTS = {
    'search_instruments': {
        'projection': [
            'symbol-search',
            'symbol-regex',
            'desc-search',
            'desc-regex',
            'search' 'fundamental',
        ]
    },
    'get_market_hours': {'markets': ['EQUITY', 'OPTION', 'FUTURE', 'BOND', 'FOREX']},
    'get_movers': {
        'market': [
            "$DJI",
            "$COMPX",
            "$SPX",
            "NYSE",
            "NASDAQ",
            "OTCBB",
            "INDEX_ALL",
            "EQUITY_ALL",
            "OPTION_ALL",
            "OPTION_PUT",
            "OPTION_CALL",
        ],
        'sort': ["VOLUME", "TRADES", "PERCENT_CHANGE_UP", "PERCENT_CHANGE_DOWN"],
        'frequency': [0, 1, 5, 10, 30, 60],
    },
    'get_user_principals': {
        'fields': [
            'streamerSubscriptionKeys',
            'streamerConnectionInfo',
            'preferences',
            'surrogateIds',
        ]
    },
}

VALID_CHART_VALUES = {
    'minute': {'day': [1, 2, 3, 4, 5, 10]},
    'daily': {'month': [1, 2, 3, 6], 'year': [1, 2, 3, 5, 10, 15, 20], 'ytd': [1]},
    'weekly': {'month': [1, 2, 3, 6], 'year': [1, 2, 3, 5, 10, 15, 20], 'ytd': [1]},
    'monthly': {'year': [1, 2, 3, 5, 10, 15, 20]},
}
