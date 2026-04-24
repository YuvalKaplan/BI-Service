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
    return CategorizeTickerItem(
        symbol=t.symbol,
        style_type=t.style_type,
        sector=t.sector,
        market_cap=t.market_cap,
        factors=t.factors,
    )


def train_model(categorized_tickers: list[CategorizeTickerItem]):
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

    def classify_symbols(self, symbols: list[str]) -> tuple[list[dict], list[str]]:
        rows = []
        no_data_symbols = []
        total = len(symbols)
        for count, symbol in enumerate(symbols, start=1):
            log.record_status(f"Classifying {symbol} with model ({count} out of {total})")
            try:
                profile, factors = fetch_company_factors(symbol)
                if not factors:
                    no_data_symbols.append(symbol)
                    continue
                feature_row = {k: factors.get(k) for k in FEATURE_COLS}
                feature_row["symbol"] = symbol
                rows.append(feature_row)
            except Exception:
                continue

        if not rows:
            return [], no_data_symbols

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

        return results, no_data_symbols


def get_classifier(categorized_tickers: list[CategorizeTickerItem]) -> StyleClassifier:
    model = train_model(categorized_tickers)
    return StyleClassifier(model)


def _run_style_stage(label: str, tickers: list, classifier: 'StyleClassifier', ticker_module) -> None:
    log.record_status(f"Style classification [{label}]: {len(tickers)} tickers to classify.")
    if not tickers:
        return
    symbol_to_id = {t.symbol: t.id for t in tickers}
    results, no_data_symbols = classifier.classify_symbols(list(symbol_to_id))
    updates = [
        {"ticker_id": symbol_to_id[r["symbol"]], "style_type": r["style"]}
        for r in results if r["symbol"] in symbol_to_id
    ]
    ticker_module.update_style_from_model_bulk(updates)
    failed_ids = [symbol_to_id[s] for s in no_data_symbols if s in symbol_to_id]
    ticker_module.update_style_factors_failed_at_bulk(failed_ids)
    log.record_status(
        f"Style classification [{label}] done: {len(updates)} classified, "
        f"{len(failed_ids)} had no FMP data."
    )


def mark_style(classifier: StyleClassifier, ticker_module) -> None:
    _run_style_stage("new", ticker_module.fetch_new_tickers_for_style(), classifier, ticker_module)
    _run_style_stage("retry", ticker_module.fetch_retry_tickers_for_style(), classifier, ticker_module)
