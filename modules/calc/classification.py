import log
import pandas as pd
from dataclasses import dataclass
from typing import Dict, Any
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.ensemble import GradientBoostingClassifier
from modules.core.api_stocks import fetch_company_factors

FEATURE_COLS = [
    "market_cap",
    "sector",
    "industry",
    "revenue_growth",
    "gross_profit_growth",
    "eps_growth",
    "ebitda_growth",
    "operating_income_growth",
    "net_income_growth",
    "asset_growth",
    "fcf_growth",
    "gross_margin",
    "operating_margin",
    "rd_ratio",
    "capex_ratio",
    "pe",
    "pb",
    "ps",
    "earnings_yield",
    "fcf_yield",
    "dividend_yield",
    "book_to_price",
    "sales_to_price",
    "cashflow_to_price"
]


@dataclass
class CategorizeTickerItem:
    symbol: str
    style_type: str | None
    sector: str
    market_cap: int
    factors: Dict[str, Any]

def to_categorize_ticker_item(t) -> CategorizeTickerItem:
    """Convert any categorize_ticker dataclass (live or BT) to the shared CategorizeTickerItem used by the classifier."""
    return CategorizeTickerItem(
        symbol=t.symbol,
        style_type=t.style_type,
        sector=t.sector,
        market_cap=t.market_cap,
        factors=t.factors,
    )


def update_factor_cache(symbols: list[str]) -> list[dict]:
    """Download company factors from the API and return the updates as a list of dicts."""
    updates = []
    total = len(symbols)

    for count, symbol in enumerate(symbols, start=1):
        try:
            log.record_status(f"Downloading factors for {symbol} ({count} out of {total})")
            profile, factors = fetch_company_factors(symbol)

            if not factors:
                continue

            updates.append({
                "symbol": symbol,
                "sector": profile.get("sector") or "Unknown",
                "market_cap": profile.get("market_cap") or 0,
                "factors": {k: factors.get(k) for k in FEATURE_COLS},
            })

        except Exception:
            continue

    return updates


def train_model(categorized_tickers: list[CategorizeTickerItem]):
    """Train a GradientBoosting style classifier from a list of categorized tickers."""
    rows = []
    for t in categorized_tickers:
        if t.style_type not in ("growth", "value"):
            continue
        if not t.factors:
            continue
        row = {
            "symbol": t.symbol,
            "style": t.style_type,
            "sector": t.sector,
            "market_cap": t.market_cap,
            **(t.factors or {})
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    categorical_features = ["sector", "industry"]
    numeric_features = [c for c in FEATURE_COLS if c not in categorical_features]

    missing_cols = [c for c in numeric_features + categorical_features if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in training data: {missing_cols}")

    preprocessor = ColumnTransformer([
        ("num", SimpleImputer(strategy="median"), numeric_features),
        ("cat", Pipeline([
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("encoder", OneHotEncoder(handle_unknown="ignore"))
        ]), categorical_features)
    ])

    model = Pipeline([
        ("prep", preprocessor),
        ("gbm", GradientBoostingClassifier(
            n_estimators=400,
            learning_rate=0.05,
            max_depth=3,
            random_state=42
        ))
    ])

    X = df[numeric_features + categorical_features]
    y = df["style"].map({"value": 0, "growth": 1})

    model.fit(X, y)
    return model


class StyleClassifier:

    def __init__(self, model):
        self.model = model

    def classify_symbols(self, symbols: list[str]):
        rows = []
        count = 0
        for symbol in symbols:
            count += 1
            print(f"Classifying {symbol} with model ({count} out of {len(symbols)})")

            profile, factors = fetch_company_factors(symbol)

            if not factors:
                continue

            feature_row = {k: factors.get(k) for k in FEATURE_COLS}
            feature_row["symbol"] = symbol
            rows.append(feature_row)

        if not rows:
            return []

        df = pd.DataFrame(rows)
        probs = self.model.predict_proba(df[FEATURE_COLS])[:, 1]

        results = []
        for i, symbol in enumerate(df["symbol"]):
            p = probs[i]
            if p > 0.55:
                style = "growth"
            elif p < 0.35:
                style = "value"
            else:
                style = "blend"
            results.append({
                "symbol": symbol,
                "style": style,
                "growth_probability": float(p)
            })

        return results


def get_classifier(categorized_tickers: list[CategorizeTickerItem]) -> StyleClassifier:
    model = train_model(categorized_tickers)
    return StyleClassifier(model)
