"""
The cron's week (service_cron.py), and the generation day the live generators and the sim share.

service_cron.py runs once a day early in the UTC morning - the evening before in New York - so
every run works on the previous trading day's close (pricing.latest_value_date):

  the daily run days  DAILY_RUN_DAYS, each after a US trading day's close: holdings, the screener,
                      ticker maintenance
  the weekly day      WEEKLY_WEEKDAY: style reference ETFs, ESG, the SEC active ETF list
  the generation day  GENERATION_WEEKDAY, after that day's daily steps: index funds -> free float ->
                      companies -> benchmarks -> best ideas -> funds

The generation day moves when the close it would work on is an NYSE holiday (Tuesday 2028-07-04
for a Wednesday): to a day later, else a day earlier (GENERATION_SHIFTS) - always within its
Monday-Sunday week and on a day the daily steps run, since the generators use that day's stored
screen and values. A holiday on the run day itself is harmless: its close isn't used. The holidays
come from FMP (holidays-by-exchange); a year it can't answer for keeps the preferred day.
"""
import log
from datetime import date, datetime, time, timedelta, timezone
from modules.core import api_stocks
from modules.ticker import pricing

MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY = range(7)
DAILY_RUN_DAYS = (TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY)   # the daily run days: each follows a US trading day's close
WEEKLY_WEEKDAY = SUNDAY          # the weekly day: style reference ETFs, ESG, the SEC active ETF list
GENERATION_WEEKDAY = WEDNESDAY    # the generators' preferred run day (on Tuesday's close)
GENERATION_SHIFTS = (1, -1)       # tried in order when its close is a holiday: a day later, else a day earlier
HOLIDAY_EXCHANGE = 'NYSE'

assert all(GENERATION_WEEKDAY + s in DAILY_RUN_DAYS for s in (0, *GENERATION_SHIFTS)), \
    "The generation day and its shifts must be days the daily steps run (the generators use their screen and values)"

_holiday_cache: dict[int, dict[date, str]] = {}   # year -> {date: name} of HOLIDAY_EXCHANGE's full closures


def week_start(d: date) -> date:
    """The Monday of d's week. A generation never moves out of its week, so two generation days
    are whole weeks apart (funds_update.activate_fund counts a fund's recalculation in weeks)."""
    return d - timedelta(days=d.weekday())


def market_day(run_day: date) -> date:
    """The close a cron run on run_day works on: the previous weekday (pricing.latest_value_date
    at the run's early UTC hour, the evening before in New York)."""
    return pricing.latest_value_date(datetime.combine(run_day, time(0), timezone.utc))


def holidays(start: date, end: date) -> dict[date, str]:
    """{date: name} of HOLIDAY_EXCHANGE's full closures over [start, end] - an early close is a
    trading day - read from FMP a calendar year at a time and cached. A year FMP can't answer for is
    left out (logged, not cached), so only the weekday rule applies to it. FMP's `from` is
    exclusive (from=2025-01-01 leaves out New Year's Day), so each year is asked for with a week
    either side and only its own dates are kept."""
    closed: dict[date, str] = {}
    for year in range(start.year, end.year + 1):
        if year not in _holiday_cache:
            rows = api_stocks.get_exchange_holidays(HOLIDAY_EXCHANGE, date(year - 1, 12, 24), date(year + 1, 1, 7))
            if isinstance(rows, str):
                log.record_notice(f"{HOLIDAY_EXCHANGE} holidays of {year} unavailable, the generation day isn't moved for them: {rows}")
                continue
            year_closed: dict[date, str] = {}
            for r in rows:
                try:
                    d = date.fromisoformat(r['date'])
                    if d.year == year and r.get('isClosed') is True:
                        year_closed[d] = r.get('name') or 'holiday'
                except (KeyError, TypeError, ValueError):
                    continue
            _holiday_cache[year] = year_closed
        closed.update({d: name for d, name in _holiday_cache[year].items() if start <= d <= end})
    return closed


def _candidates(d: date) -> list[date]:
    """d's week's possible generation days, in order of preference."""
    preferred = week_start(d) + timedelta(days=GENERATION_WEEKDAY)
    return [preferred + timedelta(days=s) for s in (0, *GENERATION_SHIFTS)]


def _pick(candidates: list[date], closed: dict[date, str]) -> date:
    """The first candidate whose close is a trading day - the preferred one when none is."""
    return next((c for c in candidates if market_day(c) not in closed), candidates[0])


def _week(d: date) -> tuple[list[date], dict[date, str]]:
    candidates = _candidates(d)
    closes = [market_day(c) for c in candidates]
    return candidates, holidays(min(closes), max(closes))


def generation_day(d: date) -> date:
    """The generation day of d's week: GENERATION_WEEKDAY, else the first of its shifts whose
    close isn't a holiday."""
    return _pick(*_week(d))


def is_generation_day(d: date) -> bool:
    return generation_day(d) == d


def generation_days_between(start: date, end: date) -> list[date]:
    """Every generation day in [start, end] (the sim), with the holidays read once for the range."""
    first = week_start(start)
    closed = holidays(first - timedelta(days=7), week_start(end) + timedelta(days=6))
    days = []
    week = first
    while week <= end:
        day = _pick(_candidates(week), closed)
        if start <= day <= end:
            days.append(day)
        week += timedelta(days=7)
    return days


def generation_note(d: date) -> str | None:
    """Why d's week generates on another day than GENERATION_WEEKDAY (for the cron email) - None
    when it doesn't."""
    candidates, closed = _week(d)
    preferred, close = candidates[0], market_day(candidates[0])
    if close not in closed:
        return None
    day = _pick(candidates, closed)
    if day == preferred:
        return (f"Generation on {preferred:%a} {preferred} as usual, though its close ({close:%a} {close}, "
                f"{closed[close]}) and every other candidate's falls on a {HOLIDAY_EXCHANGE} closure")
    return (f"Generation moved from {preferred:%a} {preferred} to {day:%a} {day}: {HOLIDAY_EXCHANGE} was closed on "
            f"{close:%a} {close} ({closed[close]})")
