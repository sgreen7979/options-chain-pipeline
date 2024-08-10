#!/usr/bin/env python3
from daily.options.mcal.parallel import MarketCalendar  # noqa

# import datetime as dt

# import pandas_market_calendars as mcal

# from daily.utils.cal import UtcOffset
# from daily.options.mcal.calendars import MarketCalendars


# class MarketCalendarHandler:

#     end_date_default_timedelta = dt.timedelta(days=365)
#     TRADING_HOURS_PER_DAY = 6.5
#     TRADING_DAYS_PER_YEAR = 252
#     TRADING_HOURS_PER_YEAR = TRADING_HOURS_PER_DAY * TRADING_DAYS_PER_YEAR

#     @staticmethod
#     def to_utc(current_time: dt.datetime):
#         return UtcOffset().toUtc(current_time).replace(tzinfo=dt.timezone.utc)

#     def __init__(self, name: MarketCalendars = MarketCalendars.NYSE):
#         self.name = name.value
#         self.set_current_time(self.now())
#         self.set_start_date(self.current_time.date())
#         self.set_end_date((self.current_time + self.end_date_default_timedelta).date())
#         self.schedule = self._get_schedule()

#     @property
#     def calendar(self):
#         return mcal.get_calendar(self.name)

#     def now(self):
#         return dt.datetime.now()

#     def _get_schedule(self):
#         schedule = self.calendar.schedule(
#             start_date=self.start_date, end_date=self.end_date
#         )

#         # Reset the index and rename the current index column to 'datetime'
#         # We want to be able to query the table using references to
#         # ISO-formatted date strings, not pd.Timestamp objects
#         schedule.reset_index(inplace=True)
#         schedule.rename(columns={'index': 'datetime'}, inplace=True)

#         # Create a new column with ISO-formatted date strings
#         schedule['iso_date'] = schedule['datetime'].dt.strftime('%Y-%m-%d')

#         # Apply the function to calculate the hours column
#         schedule['hours'] = schedule.apply(
#             lambda row: self.calculate_hours(row, self.current_time), axis=1
#         )
#         schedule['T'] = schedule['hours'].cumsum() / 6.5 / 252

#         # Rorder columns
#         schedule = schedule[
#             ["iso_date", "market_open", "market_close", "T", "hours", "datetime"]
#         ]

#         # Set the 'iso_date' column as the index
#         schedule.set_index('iso_date', inplace=True)

#         return schedule

#     @staticmethod
#     def calculate_hours(row, current_time):
#         if row['datetime'].date() > current_time.date():
#             return (row['market_close'] - row['market_open']).total_seconds() / 3600
#         elif row['datetime'].date() == current_time.date():
#             start = max(current_time, row['market_open'])
#             end = row['market_close']
#             if end <= start:
#                 return 0
#             return (end - start).total_seconds() / 3600
#         else:
#             return 0

#     def dte_to_t(self, dte: int):
#         end_date = self._end_date_from_dte(dte)
#         return self.schedule.loc[end_date.isoformat()]["T"]

#     def __call__(self, dte: int):
#         return self.dte_to_t(dte)

#     def _end_date_from_dte(self, dte: int) -> dt.date:
#         return (self.current_time + dt.timedelta(days=dte)).date()

#     def update(self, current_time: dt.datetime) -> None:
#         self.set_current_time(current_time)
#         self.schedule['hours'] = self.schedule.apply(
#             lambda row: self.calculate_hours(row, self.current_time), axis=1
#         )
#         self.schedule['T'] = (
#             self.schedule['hours'].cumsum() / self.TRADING_HOURS_PER_YEAR
#         )

#     def set_current_time(self, current_time: dt.datetime):
#         self.current_time = self.to_utc(current_time)
#         # return self

#     def set_start_date(self, start_date: dt.date):
#         self.start_date = start_date

#     def set_end_date(self, end_date: dt.date):
#         self.end_date = end_date
