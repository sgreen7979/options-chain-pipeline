#!/usr/bin/env python3
import datetime as dt
from enum import Enum
import re
import requests
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Union
import urllib.parse

from bs4 import BeautifulSoup
from bs4 import Tag
import pandas as pd

from options_chain_pipeline.lib.cal import get_prior_bizday
from options_chain_pipeline.lib.cal import is_bizday
from options_chain_pipeline.lib.utils.logging import get_logger

from ..types import MarketDataType
from ..types import DateValueMonthType
from ..types import URLNameType

HOME = "https://home.treasury.gov"
INTEREST_RATE_BASE_URL = (
    f"{HOME}/resource-center/data-chart-center/interest-rates/TextView?"
)

logger = get_logger("options_chain_pipeline.lib.term_structure.market_data.treasury", ch=True, level="INFO")


class Endpoints(Enum):
    """
    Treasury endpoints for daily interest rate data.

    Each member represents a particular endpoint and is set to the
    'type' parameter of the request.

    Use the :meth:`get_endpoint` class method to get the
    properly encoded url endpoint.
    """

    PAR_YIELD_CURVE_RATES = "daily_treasury_yield_curve"
    BILL_RATES = "daily_treasury_bill_rates"
    LT_RATES_AND_EXTRAPOLATION_FACTORS = "daily_treasury_long_term_rate"
    REAL_LT_RATE_AVGS = "daily_treasury_real_long_term"

    @classmethod
    def get_endpoint(
        cls,
        rate_type: Union[URLNameType, "Endpoints"],
        date_value_month: Optional[DateValueMonthType] = None,
    ) -> str:
        """Returns a properly encoded url.

        If no date_value_month argument is provided, it will default to the
        current month.

        :param rate_type: the interest rate data type representing the endpoint
        :type rate_type: Union[URLNameType, Endpoints]
        :param date_value_month: the time parameter, defaults to None
        :type date_value_month: Optional[DateValueMonthType], optional
        :raises ValueError: if an invalid rate_type is passed
        :return: the url
        :rtype: str
        """
        try:
            if isinstance(rate_type, str):
                type = cls[rate_type].value
            else:
                type = rate_type.value
        except KeyError as e:
            raise ValueError(f"{rate_type} is not a member of {cls.__name__}") from e
        else:
            return cls._build_url(type, date_value_month)

    @classmethod
    def _encode_params(cls, type: str, date_value_month: str) -> str:
        return urllib.parse.urlencode(
            {
                "type": type,
                "field_tdr_date_value_month": date_value_month,
            }
        )

    @classmethod
    def _build_url(
        cls, type: str, date_value_month: Optional[DateValueMonthType]
    ) -> str:
        dvm = cls._sanitize_dvm(date_value_month)
        params = cls._encode_params(type, dvm)
        return INTEREST_RATE_BASE_URL + params

    @classmethod
    def _sanitize_dvm(cls, dvm: Optional[DateValueMonthType]) -> str:
        """Validate and sanitize the date_value_month parameter.

        :param dvm: the date_value_month
        :type dvm: Optional[DateValueMonthType]
        :return: sanitized date_value_month parameter
        :rtype: str
        """
        if dvm is None:
            today = dt.date.today()
            if not is_bizday(today):
                today = get_prior_bizday()
            dvm = today.strftime("%Y%m")
        elif isinstance(dvm, str):
            if not _validate_date_value_month_str(dvm):
                raise ValueError(
                    "date_value_match passed as a str must contain "
                    "6 integers in YYYYMM format (e.g., '202405' "
                    f"for May, 2024); got '{dvm}'"
                )
            if _is_future_dt_str(dvm):
                raise ValueError("Cannot fetch data for future dates")

        elif isinstance(dvm, (dt.date, dt.datetime)):
            if _is_future_dt(dvm):
                raise ValueError("Cannot fetch data for future dates")
            dvm = dvm.strftime("%Y%m")
        else:
            raise TypeError(
                "date_value_month type must be one of "
                "None, dt.date, dt.datetime, or str, got "
                f"{type(dvm).__name__}"
            )
        return dvm

    # @classmethod
    # def _rollback_date_value_month(cls, date_value_month: str) -> str:
    #     year = int(date_value_month[:4])
    #     month = int(date_value_month[4:])
    #     if month == 1:
    #         year -= 1
    #         month = 12
    #     else:
    #         month -= 1
    #     return str(year) + str(month).zfill(2)


def _validate_date_value_month_str(date_value_month: str) -> bool:
    pat = re.compile(r"\d{6}")
    return pat.fullmatch(date_value_month) is not None


def _is_future_dt(date_value_month: Union[dt.date, dt.datetime]) -> bool:
    """Checks if date_value_month is a future date or datetime"""
    today = dt.date.today
    now = dt.datetime.now
    current = today() if isinstance(date_value_month, dt.date) else now()
    return date_value_month > current


def _is_future_dt_str(date_value_month: str) -> bool:
    year_month = int(date_value_month)
    current = int(dt.date.today().strftime("%Y%m"))
    return year_month > current


def fetch_treasury_yield_curve(url: str) -> pd.DataFrame:
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Locate the table containing the yield data
    table_class = "usa-table views-table views-view-table cols-23"
    table = soup.find("table", {"class": table_class})
    assert isinstance(table, Tag)  # mypy

    # Extract headers and rows
    headers = _extract_table_headers(table)
    rows = _extract_table_rows(table)

    # Create a DataFrame
    df = pd.DataFrame(rows, columns=headers)

    # Convert 'Date' column data types to datetime
    df['Date'] = pd.to_datetime(df['Date'])

    # Drop all null columns, including columns
    # with values all equal to "N/A"
    df = _drop_null_cols(df)

    # Convert data types of all columns, excluding
    # the "Date" column, to float
    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def get_market_data_for_yield_curve(
    ref_date: Optional[dt.date] = None,
) -> Tuple[dt.date, MarketDataType]:
    # yr_mo = dt.date.today().strftime("%Y%m")

    # # URL for DAILY TREASURY PAR YIELD CURVE RATES
    # url = f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?type=daily_treasury_yield_curve&field_tdr_date_value_month={yr_mo}"

    if ref_date is not None:
        logger.info(f"ref_date passed: {ref_date.isoformat()}")
        date_value_month = ref_date.strftime("%Y%m")
    else:
        logger.info("no ref_date passed")
        date_value_month = get_prior_bizday().strftime("%Y%m")

    logger.info(f"date_value_month: {date_value_month}")

    url = Endpoints.get_endpoint(
        Endpoints.PAR_YIELD_CURVE_RATES, date_value_month=date_value_month
    )

    logger.info(f"fetching yield curve data from {url}")

    # Fetch the latest treasury yields
    df = fetch_treasury_yield_curve(url)

    # if a ref_date was passed
    if ref_date is not None:
        reference_date: pd.Timestamp = pd.Timestamp(ref_date)
        if reference_date in df["Date"].values.tolist():
            # reference_date found in df["Date"] column
            logger.info(f"reference_date found: {reference_date.date().isoformat()}")
            idx = df[df["Date"] == reference_date].index.tolist()[0]
            latest_data = df.iloc[idx]
        else:
            # reference_date not found in df["Date"] column
            logger.info(
                f"reference_date not found: {reference_date.date().isoformat()}"
            )
            if df[df["Date"] < reference_date].values.tolist():
                # setting reference_date to the next earliest date present
                latest_data: pd.Series = df[df["Date"] < reference_date].iloc[-1]
                reference_date: pd.Timestamp = latest_data["Date"]
                logger.info(
                    f"setting reference_date to next earliest date: {reference_date.date().isoformat()}"
                )
            elif df[df["Date"] > reference_date].values.tolist():
                # setting reference_date to the next date present
                latest_data: pd.Series = df[df["Date"] > reference_date].iloc[0]
                reference_date: pd.Timestamp = latest_data["Date"]
                logger.info(
                    f"setting reference_date to next date present: {reference_date.date().isoformat()}"
                )
    else:
        # Select the most recent date's yields
        latest_data: pd.Series = df.iloc[-1]
        reference_date: pd.Timestamp = latest_data['Date']
        logger.info(
            f"setting reference_date to latest date present: {reference_date.date().isoformat()}"
        )

    # # Select the most recent date's yields
    # latest_data: pd.Series = df.iloc[-1]
    # reference_date: pd.Timestamp = latest_data['Date']

    # Extracting data points (maturities and corresponding yields)
    maturities = list(df.columns[1:])

    data_points = []
    for maturity in maturities:
        yield_rate = latest_data[maturity]
        if pd.notnull(yield_rate):
            maturity_date = _get_maturity_date(maturity, reference_date)
            data_points.append((maturity_date, yield_rate / 100.0))

    return reference_date.date(), data_points


def get_market_data_for_yield_curve_NEW(
    ref_date: Optional[dt.date] = None,
    # ) -> Tuple[dt.date, List[Tuple[dt.date, float]]]:
) -> Tuple[dt.date, MarketDataType]:
    """
    Fetches and processes yield curve data for a given reference date.

    Parameters:
    ref_date (Optional[dt.date]): The reference date for which to fetch the yield curve data.

    Returns:
    Tuple[dt.date, List[Tuple[dt.date, float]]]: A tuple containing the reference date and
    a list of tuples with maturity dates and corresponding yield rates.
    """
    date_value_month = (
        ref_date.strftime("%Y%m") if ref_date else get_prior_bizday().strftime("%Y%m")
    )
    logger.info(f"Fetching data for date_value_month: {date_value_month}")

    url = Endpoints.get_endpoint(
        Endpoints.PAR_YIELD_CURVE_RATES, date_value_month=date_value_month
    )
    logger.info(f"Fetching yield curve data from {url}")

    try:
        df = fetch_treasury_yield_curve(url)
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        raise

    reference_date = pd.Timestamp(ref_date) if ref_date else df["Date"].iloc[-1]
    latest_data = _get_latest_data_for_date(df, reference_date)
    reference_date = latest_data["Date"]
    # maturities, data_points = _extract_data_points(latest_data, reference_date)
    data_points = _extract_data_points(latest_data, reference_date)

    return reference_date.date(), data_points


def _get_latest_data_for_date(
    df: pd.DataFrame, reference_date: pd.Timestamp
) -> pd.Series:
    """
    Retrieves the latest data for the specified reference date from the DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame containing yield curve data.
    reference_date (pd.Timestamp): The reference date for which to retrieve the data.

    Returns:
    pd.Series: The row of data corresponding to the reference date.
    """
    if reference_date in df["Date"].values.tolist():
        # idx = df[df["Date"] == reference_date].index[0]
        idx = df[df["Date"] == reference_date].index.tolist()[0]
        logger.info(f"Reference date found: {reference_date.date()}")
        return df.iloc[idx]
    else:
        logger.info(f"Reference date not found: {reference_date.date()}")
        earlier_dates = df[df["Date"] < reference_date]
        later_dates = df[df["Date"] > reference_date]

        if not earlier_dates.empty:
            logger.info(
                f"Setting reference date to next earliest date: {earlier_dates.iloc[-1]['Date'].date()}"
            )
            return earlier_dates.iloc[-1]
        elif not later_dates.empty:
            logger.info(
                f"Setting reference date to next date present: {later_dates.iloc[0]['Date'].date()}"
            )
            return later_dates.iloc[0]
        else:
            raise ValueError("No valid dates found in the data")


def _extract_data_points(
    latest_data: pd.Series,
    reference_date: pd.Timestamp,
    # ) -> Tuple[List[str], List[Tuple[dt.date, float]]]:
) -> MarketDataType:
    # ) -> Sequence[Tuple[dt.date, float]]:
    """
    Extracts maturities and corresponding yield rates from the latest data row.

    Parameters:
    latest_data (pd.Series): The row of data for the reference date.
    reference_date (pd.Timestamp): The reference date for the data.

    Returns:
    Tuple[List[str], List[Tuple[dt.date, float]]]: A tuple containing maturities and a list of tuples with maturity dates and yield rates.
    """
    maturities = list(latest_data.index[1:])
    data_points = [
        (
            _get_maturity_date(maturity, reference_date),
            (latest_data[maturity] / 100.0),
        )
        for maturity in maturities
        if pd.notnull(latest_data[maturity])
    ]
    return data_points  # type: ignore


def _extract_table_headers(table: Tag) -> List[str]:
    """Extracts headers of the passed table"""
    return [th.text.strip() for th in table.find_all('th')]


def _extract_row_cols(tr: Tag) -> List[str]:
    return [td.text.strip() for td in tr.find_all('td')]


def _extract_table_rows(table: Tag) -> List[str]:
    """Extracts data rows from the passed table"""
    rows = []
    for tr in table.find_all('tr')[1:]:
        if cols := _extract_row_cols(tr):
            rows.append(cols)
    return rows


def _isnull(col_data: pd.Series) -> bool:
    """Returns a bool indicating whether the column is null

    We say a column is null if its col_data consists of values
    all equal to null (np.nan?) or "N/A
    """
    return all(col_data.isnull()) or all(col_data == "N/A")


def _drop_null_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop all null columns, including columns whose values
    are all equal to "N/A"
    """
    for col in df.columns[1:]:
        try:
            col_data: pd.Series = df[col]
        except KeyError:
            # column already dropped -- occurs when
            # multiple columns share the same name
            continue
        else:
            if _isnull(col_data):
                df = df.drop([col], axis=1)
    return df


def _get_days_to_maturity(maturity: str) -> int:
    msplit = maturity.split()
    n_periods = int(msplit[0])
    period_type = msplit[1].strip()
    period_type_lower = period_type.lower()

    if period_type_lower == "yr":
        days_per_period = 365
    elif period_type_lower == "mo":
        days_per_period = 30
    else:
        raise ValueError(
            "unrecognized period type; expected "
            "'YR'/'Yr'/'yr' or 'MO'/'Mo'/'mo', got "
            f"'{period_type}'"
        )

    return n_periods * days_per_period


def _get_timedelta(maturity: str) -> dt.timedelta:
    return dt.timedelta(days=_get_days_to_maturity(maturity))


def _get_maturity_date(maturity: str, reference_date: pd.Timestamp) -> dt.date:
    return (reference_date + _get_timedelta(maturity)).date()


def _validate_url_data(url: str) -> bool:
    """
    Checks if the url coinciding with a time period (year and month)
    is valid.

    Helpful when the date_value_month parameter represents a future
    time.

        .. NOTE: now that validators are in place to ensure
        date_value_month is not a future time, this function is obsolete
    """
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    try:
        text = soup.find("div", {"class": "view-empty"}).text.strip()  # type: ignore
    except AttributeError:
        return True
    else:
        return text == "No Results Found."
