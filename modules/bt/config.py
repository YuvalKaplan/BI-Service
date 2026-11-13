"""
Backtesting's own copies of the values it shares with the live pipeline (modules/config.py), so
tuning one pipeline never changes the other. Values only BT uses stay next to their BT code.
BT code refers to them as config.<Section>.<NAME> (from modules.bt import config).
"""


class BestIdeas:
    """The active-weight algorithm (modules/bt/actions/best_ideas_generator.py)."""
    MIN_HOLDINGS_WITH_PRICES_PCT = 0.9    # share of an ETF's holdings that need a price for its best ideas
    MAX_BEST_IDEAS_PER_FUND = 10          # best ideas per ETF
    HOLDING_DELTA_LIMIT_DROP_OFF = 0.25   # an active weight above this is left out


class FundStrategy:
    """Market-cap weighting (modules/bt/calc/model_fund.py)."""
    MC_WEIGHT_ALPHA = 0.5       # power-law exponent: 1.0 = pure market-cap, 0.0 = equal-weight
    MC_WEIGHT_CAP = 0.10        # maximum weight per holding
    MC_WEIGHT_FLOOR = 0.01      # minimum weight per holding


class Style:
    """Value / growth classification (modules/bt/calc/classification.py)."""
    MODEL_PARAMS = {'n_estimators': 400, 'learning_rate': 0.05, 'max_depth': 3, 'random_state': 42}
    GROWTH_MIN_PROBABILITY = 0.55  # a growth probability above this is 'growth'...
    VALUE_MAX_PROBABILITY = 0.35   # ... below this 'value'; in between 'blend'


class Esg:
    """ESG qualification (modules/calc/esg.py's qualify, called with these rules by
    modules/bt/actions/stocks_download.py): a missing factor passes, all missing fails."""
    PASSING_GRADES = {"AAA", "AA", "A", "BBB", "BB", "B"}  # FMP ESG risk rating
    ESG_SCORE_MIN = 50.0
    GOVERNANCE_SCORE_MIN = 50.0
