"""
The live pipeline's configuration: every tunable value the modules use (thresholds, windows,
schedule, rate limits, worker counts, exchange lists) and the fund strategy's defaults, in one
place. Grouped by the stage that reads them; code refers to them as config.<Section>.<NAME>.

Not here: secrets (.env), backtesting (modules/bt/config.py holds BT's copies of the values it
shares with the live pipeline), scripts' own run settings (edited at the top of each script),
identifiers / enums, SQL and the parsing rules (regular expressions) that live next to their code.

Changing a value that the methodology quotes (methodology/detailed.md, Appendix A) means updating
that document and regenerating the strategy documents too.
"""
import calendar
import os
from datetime import date


# ── External services ────────────────────────────────────────────────────────

class Api:
    """FinancialModelingPrep client (modules/core/api_stocks.py) - shared by live and BT, since
    the rate limit is the FMP account's."""
    FMP_API_URL = 'https://financialmodelingprep.com/stable'
    CALLS_PER_MINUTE = 200           # token bucket: at most this many calls...
    WINDOW_SECONDS = 60.0            # ... in this rolling window
    API_RETRIES = 3
    API_RETRY_DELAY = 2.0            # seconds; doubles on each subsequent attempt
    SHARES_FLOAT_PAGE_LIMIT = 5000   # shares-float-all page size
    SCREENER_PAGE_LIMIT = 1000       # company-screener page size...
    SCREENER_MAX_PAGES = 20          # ... and pages per exchange


class Edgar:
    """SEC EDGAR client (modules/sec/edgar.py)."""
    EDGAR_ARCHIVES_URL = 'https://www.sec.gov/Archives/edgar'
    MIN_REQUEST_INTERVAL = 0.2  # seconds between requests: 5 per second
    API_RETRIES = 3
    API_RETRY_DELAY = 2.0       # seconds; doubles on each subsequent attempt
    REQUEST_TIMEOUT = 60        # seconds


class Sender:
    """Admin email (modules/core/sender.py, via Mailgun)."""
    DOMAIN = 'insightful.investments'
    FROM = f"Best Ideas Admin <admin@{DOMAIN}>"
    TO = f"yuval.kaplan@{DOMAIN}"
    CC = f"oren.kaplan@{DOMAIN}"


# ── Schedule ─────────────────────────────────────────────────────────────────

class Schedule:
    """The cron's week (modules/cron/schedule.py). Each run works on the previous trading day's
    close, so a daily run day follows a US trading day."""
    DAILY_RUN_DAYS = (calendar.TUESDAY, calendar.WEDNESDAY, calendar.THURSDAY, calendar.FRIDAY, calendar.SATURDAY)
    WEEKLY_WEEKDAY = calendar.SUNDAY          # the weekly day: style reference ETFs, ESG
    GENERATION_WEEKDAY = calendar.WEDNESDAY   # the generators' preferred run day (on Tuesday's close)
    SEC_LIST_REMINDER_DAY = 1                 # day of the month: the reminder to update the SEC active ETF list by hand
    GENERATION_SHIFTS = (1, -1)               # tried in order when its close is a holiday: a day later, else a day earlier
    HOLIDAY_EXCHANGE = 'NYSE'                 # whose full closures move the generation day


# ── SEC active ETF list, profiles and selection ──────────────────────────────

class Ncen:
    """The SEC active ETF list from N-CEN filings (modules/sec/ncen.py)."""
    WINDOW_QUARTERS = 5         # a filing year plus slack for late filers, so the first run builds the whole list
    CURRENT_WINDOW_DAYS = 455   # no filing in ~15 months (liquidated, no termination listed yet): off the current list
    MAX_FAILED_SHARE = 0.1      # more failed filings than this in a run fails the run
    MIN_FAILED_TO_RAISE = 5     # ... but a handful never does
    PROGRESS_EVERY = 250        # filings between progress lines
    DOWNLOAD_WORKERS = 4        # parallel filing downloads, all under edgar's 5-requests-per-second throttle
    FAILED_XML_DIR = os.path.join('.output', 'downloads', 'ncen')   # filings that failed to parse, kept for debugging


class EtfProfile:
    """The SEC active ETFs' FMP profiles (modules/sec/etf_profile.py)."""
    PROFILE_REFRESH_DAYS = 28   # a fund's profile is checked again after this long (or after a newer filing)
    PROFILE_REFRESH_DAYS_ACTIVE = 6  # ... an active fund's: every run
    FETCH_WORKERS = 4           # funds fetched in parallel, all under api_stocks' 200-calls-a-minute throttle
    EQUITY_MIN_WEIGHT = 0.75    # share of the fund in stock lines an equity fund needs
    MIN_COVERAGE = 0.5          # share of the fund placed (company or index fund) needed for region, size and emerging
    US_MIN = 0.8                # US share of the placed stocks for 'US'; at most 1 - US_MIN for 'International'
    LARGE_COVERAGE = 0.70       # a stock is large at or above its market's breakpoint at this coverage...
    MID_COVERAGE = 0.90         # ... mid down to this one, small below (Morningstar-style)
    LARGE_MIN = 0.7             # cap size: large share for 'large'
    SMALL_MIN = 0.5             # ... small share for 'small'
    MID_MIN = 0.5               # ... mid share for 'mid'
    SMID_MIN = 0.7              # ... mid + small share for 'smid'; otherwise 'all'
    STYLE_MIN = 0.6             # value (growth) share of the style-classified weight for 'value' ('growth')
    STYLE_MIN_COVERAGE = 0.5    # style-classified share of the stock weight needed for value / growth
    SECTOR_MIN_COVERAGE = 0.5   # sector-classified share of the stock weight needed for our own sector weights
    COUNTRY_MIN_COVERAGE = 0.5  # country-classified share of the stock weight needed for country weights
    MAX_FAILED_SHARE = 0.1      # more failed funds than this in a run fails the run...
    MIN_FAILED_TO_RAISE = 5     # ... but a handful never does


class EtfSelection:
    """The selection rules: which provider ETFs are active (modules/sec/etf_selection.py)."""
    REGIONS = ('US', 'International')
    CAP_TYPES = ('large', 'all')
    MIN_STOCK_HOLDINGS = 20
    MAX_STOCK_HOLDINGS = {'US': 200, 'International': 250}
    MAX_SECTOR_EXCESS = 0.20        # a sector's weight over its highest weight in our benchmarks (sector_reference)
    MAX_TOP_COUNTRY_WEIGHT = 0.35
    MAX_EMERGING_WEIGHT = 0.35
    MIN_COVERAGE_ENTRY = 0.70       # stock weight in the region's blend benchmark companies to become active...
    MIN_COVERAGE_STAY = 0.65        # ... and to stay active
    MAX_HOLDINGS_AGE_DAYS = 14      # an active ETF's latest holdings are at most this old
    ELN_MIN_LEVEL = 20.0            # a percentage before a date at or above this is an equity-linked note's level, not a coupon


# ── Holdings download ────────────────────────────────────────────────────────

class EtfDownloader:
    """The provider ETFs' FMP holdings (modules/cron/etf_downloader.py)."""
    FETCH_WORKERS = 4           # ETFs fetched in parallel, all under api_stocks' 200-calls-a-minute throttle
    MAX_FAILED_SHARE = 0.1      # more failed ETFs than this in a run fails the run...
    MIN_FAILED_TO_RAISE = 5     # ... but a handful never does


class Holdings:
    """Provider ETF holding lines (modules/object/provider_etf_holding.py)."""
    # Max/min ratio of implied prices (market_value / shares) under which duplicate lines of one
    # ticker are treated as lots of the same security rather than different securities.
    DUP_PRICE_TOLERANCE = 1.02


# ── Ticker utilities ─────────────────────────────────────────────────────────

class Markets:
    """Exchange and country rules behind a company's home market and region (modules/ticker/util.py,
    modules/object/ticker.py)."""
    # Exchanges whose listings make a company "US-listed" for region purposes (OTC excluded:
    # it mostly carries foreign companies' unsponsored ADRs). Cboe Global Markets is listed on its
    # own exchange (FMP: CBOE) - left out, it was the one US company called International.
    US_LISTING_EXCHANGES = {'NYSE', 'NASDAQ', 'AMEX', 'CBOE'}
    # A symbol on one of these exchanges is preferred when a holding line could be either listing
    # (ticker.is_preferred_exchange).
    PREFERRED_EXCHANGES = {'NYSE', 'NASDAQ'}
    # Countries companies are incorporated in for legal / tax reasons rather than run from (Bermuda,
    # Cayman, BVI, the Channel Islands, Isle of Man, the ship registries Liberia and the Marshall
    # Islands, the Bahamas, Curaçao, Gibraltar): neither FMP's country nor an ISIN from one tells
    # where a company belongs (Viking, XP, Nu, Credicorp, Amer Sports, Birkenstock).
    TAX_HAVEN_COUNTRIES = {'BM', 'KY', 'VG', 'JE', 'GG', 'IM', 'LR', 'MH', 'BS', 'CW', 'GI'}
    # Extra countries counted as a company's home market beyond its own (HK-listed Chinese companies).
    HOME_COUNTRY_ALIASES = {'CN': {'HK'}}
    # Dutch and Luxembourg holding companies are often primarily listed on another continental
    # exchange (Airbus and Euronext in Paris, Stellantis and Ferrari in Milan, argenx in Brussels,
    # ArcelorMittal in Amsterdam/Madrid, Tenaris in Milan), so those markets count as home for
    # them too — ranked after the domicile country's own exchanges.
    WIDER_HOME_COUNTRIES = {
        'NL': {'FR', 'BE', 'IT', 'PT', 'ES'},
        'LU': {'NL', 'FR', 'BE', 'IT', 'PT', 'ES'},
    }


class Resolver:
    """Holding line -> ticker resolution and the invalid rules (modules/ticker/resolver.py)."""
    TRADING_LOOKBACK_DAYS = 10   # a day with volume within this many days keeps a ticker FMP calls inactive valid


class Refresh:
    """Ticker profile refresh and share-count verification (modules/ticker/refresh.py)."""
    WORKERS = 5                 # tickers refreshed in parallel — FMP's rate limit (200/min, api_stocks' shared throttle) is the bound
    PROFILE_MAX_AGE_DAYS = 7    # a profile refreshed longer ago than this is refreshed again
    SHARES_CHECK_POINTS = 10    # the latest stored values stored_shares_off_profile looks at
    FINANCIALS_TOLERANCE = 0.05  # FMP's quarterly weighted shares within this of the profile's count confirm it
    INDEX_TOLERANCE = 0.15      # the index funds' float shares may exceed the quote's share count by at most this
    INDEX_OVER_HISTORY = 1.15   # index float shares this many times FMP's history count refute the history (FreeFloat.CAP_CHECK_FACTOR)
    HISTORY_COUNT_DAYS = 10     # the days of FMP's market-cap history index_backs_quote reads the history's share count from
    SAME_SHARES_TOLERANCE = 0.005  # a verified share count within this of the quote's is kept as it is


class Pricing:
    """Validated prices and market caps, FMP glitch repair, FX (modules/ticker/pricing.py)."""
    HISTORY_LOOKBACK_DAYS = 21  # calendar days - comfortably covers 10+ trading days even across a holiday-heavy stretch
    PRICE_TOLERANCE = 0.005     # 0.5% relative diff
    MARKET_CAP_TOLERANCE = 0.01  # 1.0% relative diff (looser: also depends on shares outstanding)
    GRACE_PERIOD_DAYS = 5       # consecutive calendar days a ticker may go unrecorded while mismatching before it gets flagged invalid
    REVISION_TOLERANCE = 0.05   # every mismatch within this: FMP revised its history - its values replace ours
    BASIS_SPREAD = 1.06         # only prices or only caps off, this steadily, on every mismatching day: our
    BASIS_MIN_POINTS = 5        # history is on another basis (currency, split) - rewritten; at least this many
                                # days (an exchange rate moves a few % over the days compared: NZD 3.1% in Sep 2026)
    VALUE_DATE_CUT_OFF_HOUR = 17  # New York time: before it, the latest completed trading day is the previous one
    GLITCH_FACTOR = 3.0         # a day-over-day market-cap move this large, not matched by the price, is an FMP glitch
    EXTREME_GLITCH_FACTOR = 20.0  # a move this large is a glitch even when FMP scaled the price with it (pence vs pounds)
    MAX_EDGE_GLITCH_POINTS = 65  # without prices, a jump at a series' start/end lasting longer than this (~3 months) is a real level change
    OUTLIER_CONTEXT_DAYS = 365  # history fetched around a new value so market_cap_outliers can tell what's normal
    REPAIR_REFERENCE_POINTS = 5  # good values nearest a glitch whose implied share count repairs it
    REFERENCE_TRIGGER_FACTOR = 2.0  # a cap move this large the price didn't make is worth checking against the profile
    REFERENCE_FACTOR = 2.5      # an implied share count this far off the profile's is a glitch (real share counts
                                # don't move 2.5x in a year; Alphabet's all-classes/class-A ratio is ~2.1)
    SHARE_COUNT_TOLERANCE = 1.15  # an implied share count this far off a listing's verified one (ticker.verified_shares)
                                  # is FMP's history on a wrong count (Rocket Companies: 3.79B against 2.82B)
    LATEST_RUN_TOLERANCE = 1.05  # implied share counts within this of the latest one are the same run of FMP's history
    VALUE_HISTORY_START = date(2026, 1, 1)  # start of stored ticker_value history (see scripts/data_fill_ticker_value_refresh.py)
    CLOSEST_VALUE_WINDOW_DAYS = 5  # how far from a date closest_value_for_date looks by default (FX rates)
    FX_RATE_LOOKBACK_DAYS = 10  # days of FX history fetched for a currency's latest rate


class Valuation:
    """The valuation pass (modules/ticker/valuation.py)."""
    WORKERS = 5                 # tickers valued in parallel — FMP's rate limit (200/min) is the bound
    HOLDINGS_LOOK_BACK_DAYS = EtfSelection.MAX_HOLDINGS_AGE_DAYS  # holdings best ideas can still use (BestIdeas.LOOK_BACK_WINDOW)


class Company:
    """Primary listing, company market cap and region (modules/ticker/company.py)."""
    ACTIVE_DAYS = 30            # no market cap within this many days of "as of" = inactive (e.g. a ticker left behind by a rename).
                                # Generous on purpose: many listings only get a value from the weekly screener, and a
                                # missed week or two must not flip a company's primary listing (and so its region).
    THIN_TURNOVER_SHARE = 0.05  # see company.thin_lines. Hybrids trade 0.1-3.5% of their company's main line
                                # (Southern's units SOMN 2.3%, PPL's PPLC 3.4%); share classes kept as
                                # ordinary sit above (BRK-A 6.8%, FWONA 7.3%).
    INDEX_MARKET_DOMINANCE = 2.0  # held by both markets' index funds: one market's float cap must be this
                                  # many times the other's to decide (FMP files some foreign lines under a
                                  # US company's symbol and ISIN: PLDT as TE Connectivity, Electrica as
                                  # Estée Lauder); a real dual holding (Waste Connections, RB Global) is
                                  # near 1 and falls through to the company's country.


class Identity:
    """Evidence that two listing groups are one company (modules/ticker/identity.py)."""
    ISIN_CAP_TOLERANCE = 0.10   # a shared ISIN whose names disagree still links when the caps agree this closely
    NAME_CAP_TOLERANCE = 0.02   # matching names (without a shared ISIN or CIK) link only when the caps agree this closely
    CAP_MAX_DAY_GAP = 1         # caps are compared on the same date, give or take this many days


class Master:
    """Share-class consolidation (modules/ticker/master.py)."""
    # A master is realigned to a better primary listing only when that listing has a market cap
    # within this many days — longer than Company.ACTIVE_DAYS, so a listing that misses a few weekly
    # screener runs doesn't swap the company's id back and forth.
    REALIGN_ACTIVE_DAYS = 90
    RECENT_CAP_DAYS = 21        # how far back link_same_company looks for caps to compare on a common date


class Style:
    """Value / growth classification (modules/ticker/style.py, modules/calc/classification.py)."""
    FACTORS_RETRY_DAYS = 30     # a ticker FMP had no factor data for is retried after this long...
    RETRY_BATCH_SIZE = 200      # ... this many per run
    # The GradientBoosting classifier, trained on the style reference ETFs' companies
    MODEL_PARAMS = {'n_estimators': 400, 'learning_rate': 0.05, 'max_depth': 3, 'random_state': 42}
    GROWTH_MIN_PROBABILITY = 0.55  # a growth probability above this is 'growth'...
    VALUE_MAX_PROBABILITY = 0.35   # ... below this 'value'; in between 'blend'


class Esg:
    """ESG qualification (modules/calc/esg.py): a missing factor passes, all missing fails."""
    PASSING_GRADES = {"AAA", "AA", "A", "BBB", "BB", "B"}  # FMP ESG risk rating
    ESG_SCORE_MIN = 50.0
    GOVERNANCE_SCORE_MIN = 50.0


class IndexFunds:
    """The index funds (VTI / VEA / VWO) and the market's size breakpoints (modules/ticker/index_funds.py)."""
    MIN_FLOAT_FACTOR = 0.10     # S&P's minimum float (IWF): benchmark membership and the funds' large filter
    BREAKPOINT_COVERAGES = [round(0.50 + i / 100, 2) for i in range(50)]   # 0.50 - 0.99, every week
    SCREEN_MARGIN = 0.8         # the universe screener fetches from this share of the lowest benchmark cutoff
    SNAPSHOT_MAX_AGE_DAYS = 7   # an older snapshot is refreshed before it's used
    EMERGING_FUNDS = ('VWO',)   # the index funds whose stocks are emerging markets (FTSE Emerging: Korea isn't)
    # FTSE includes China A shares (Shanghai / Shenzhen listings - VWO's .SS / .SZ lines) at 25% of
    # their investable float, so their holding is scaled up by it before it's valued: 2,077 of VWO's
    # 5,035 lines are A shares, 6% of its weight, and left as held they pulled VWO's scale ~20% up
    # (flagging South African and Brazilian caps as too low) and put the A-share companies at a
    # quarter of their float.
    CHINA_A_INCLUSION = 0.25


class FreeFloat:
    """Free float, float factors and the cap check (modules/ticker/free_float.py)."""
    CAP_CHECK_FACTOR = 1.15     # an index float cap this many times the company cap flags the cap as too low
                                # (fully floated companies sit at 0.97-1.01 — see free_float._value_holdings)
    VALUE_LOOKBACK_DAYS = 14    # stored prices and caps a holding is valued from
    FX_RATE_WINDOW_DAYS = 5     # how far from the valuation date an FX rate may be
    PRICE_WINDOW_DAYS = 3       # how far from the company cap's date our stored price may be


# ── Screen, companies and benchmarks ─────────────────────────────────────────

class Screener:
    """The FMP screener (modules/cron/screener.py)."""
    # FMP's screener has to be queried per exchange to get real international coverage (an
    # unfiltered call is heavily US/Canada-biased and mixes currencies), so the screen is the
    # union of these exchanges' large caps.
    US_SCREENER_EXCHANGES = ['NYSE', 'NASDAQ', 'AMEX']
    INTERNATIONAL_EXCHANGES = [
        'TSX', 'LSE',
        'JPX', 'XETRA', 'HKSE', 'ASX', 'SIX', 'PAR', 'SHH', 'NSE', 'TAI', 'BSE',
        'SHZ', 'STO', 'KSC', 'MIL', 'FSX', 'SAO', 'AMS', 'WSE', 'SES', 'BME',
        'OSL', 'VIE', 'JNB',
        'CPH', 'HEL', 'BRU', 'LIS', 'DUB', 'TLV', 'NZE', 'SET', 'KLS', 'IST', 'MEX', 'SAU',
    ]
    SCREENER_EXCHANGES = US_SCREENER_EXCHANGES + INTERNATIONAL_EXCHANGES


class CompanyBuilder:
    """Screened companies from the stored screen (modules/cron/company_builder.py)."""
    RECENT_CAP_DAYS = 21        # caps the duplicate guard compares on a common date
    VALUE_WINDOW_DAYS = 5       # a listing's value may be this many days older than the screen date (a market holiday)


# ── Best ideas and funds ─────────────────────────────────────────────────────

class BestIdeas:
    """The active-weight algorithm (modules/cron/best_ideas_generator.py)."""
    DAYS_NO_MARKET_CAP = 5      # a company's market cap is looked for this many days around the holdings date
    MIN_HOLDINGS_WITH_PRICES_PCT = 0.90   # share of an ETF's holdings that need a market cap for its best ideas
    LOOK_BACK_WINDOW = EtfSelection.MAX_HOLDINGS_AGE_DAYS  # an active ETF's holdings are never older
    MAX_BEST_IDEAS_PER_FUND = 10          # best ideas per ETF (ranks 1 - 10)
    HOLDING_DELTA_LIMIT_DROP_OFF = 0.20   # an active weight above this is left out (delta>limit)


class FundsUpdate:
    """Fund recalculation (modules/cron/funds_update.py, modules/object/best_idea.py)."""
    COMPANY_CAP_WINDOW_DAYS = 10  # a company's market cap for the funds' large filter and weighting: this many days back


class FundStrategy:
    """The fund strategy's defaults (modules/calc/model_fund.py: Strategy), used where a fund's
    stored strategy (fund.strategy) leaves a setting out, and the composition rules every fund shares."""
    ALLOCATION_REBALANCE = 'on_change'  # 'none' | 'on_change' | 'full'
    ESG_ONLY = False
    # Points a company earns from each ETF by its rank there (index = rank - 1; ranks beyond the
    # list earn none), summed across the ETFs into its score - the selection's order. Rank 1 = 512,
    # 2 = 256, ... 10 = 1 (each ETF's best ideas run to rank BestIdeas.MAX_BEST_IDEAS_PER_FUND): the
    # halving scale makes rank 1 worth two rank 2s, four rank 3s, ... (consensus over a single conviction).
    RANK_POINTS = [2 ** (10 - r) for r in range(1, 11)]
    # Which best ideas the fund draws on (best_idea.benchmark_mode): 'full_value_growth_split' | 'self'
    BENCHMARK = 'full_value_growth_split'
    RECALC_FREQUENCY_DAYS = 7
    # A holding is kept while it's within the top ceil(target x KEEP_BUFFER) of its bucket by score.
    KEEP_BUFFER = 1.25
    # Market-cap weighting
    MC_WEIGHT_ALPHA = 0.5       # power-law exponent: 1.0 = pure market-cap, 0.0 = equal-weight
    MC_WEIGHT_CAP = 0.10        # maximum weight per holding
    MC_WEIGHT_FLOOR = 0.01      # minimum weight per holding


# ── Simulation and reports ───────────────────────────────────────────────────

class Sim:
    """The fund simulation (modules/sim/)."""
    DEFAULT_INCEPTION_DATE = date(2026, 1, 15)
    WINDOW_DAYS = 5             # how far from a generation day to search for the closest available market cap
    DATA_LAG_DAYS = 3           # FMP's historical price/market-cap endpoints don't have data yet for the most recent days


class FundMethodology:
    """The methodology export (modules/report/fund_methodology.py)."""
    OUTPUT_ROOT = os.path.join('.output', 'methodology')
