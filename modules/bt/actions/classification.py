
import log
import pandas as pd
from typing import List
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.ensemble import GradientBoostingClassifier
from modules.core.fdata import fetch_company_factors
from modules.bt.object import categorize_ticker

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
    # "ev_ebitda",
    # "price_to_fcf",
    # "price_to_sales",
    # "price_to_operating_cf",
    "earnings_yield",
    "fcf_yield",
    "dividend_yield",
    "book_to_price",
    "sales_to_price",
    "cashflow_to_price"
]

def update_factor_cache():

    symbols = categorize_ticker.fetch_symbols()

    count = 0
    total = len(symbols)

    for symbol in symbols:
        count += 1
        try:
            print(f"Downloading factors for {symbol} ({count} out of {total})")
            
            profile, factors = fetch_company_factors(symbol)

            if not factors:
                continue

            feature_set = {k: factors.get(k) for k in FEATURE_COLS}
            sector = profile.get("sector") or "Unknown"
            market_cap = profile.get("market_cap") or 0

            categorize_ticker.update(
                symbol=symbol,
                sector=sector,
                market_cap=market_cap,
                factors=feature_set
            )

        except Exception:
            continue

def train_model():

    categorized_tickers = categorize_ticker.fetch_all()
    
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

        # row.update(t.factors)

        rows.append(row)

    df = pd.DataFrame(rows)

    print(df)

    # --- Define categorical features ---
    categorical_features = ["sector", "industry"]

    # --- Ensure they are NOT in numeric features ---
    numeric_features = [c for c in FEATURE_COLS if c not in categorical_features]

    # --- Validate (very important for debugging) ---
    missing_cols = [c for c in numeric_features + categorical_features if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in training data: {missing_cols}")

    # --- Preprocessing ---
    preprocessor = ColumnTransformer([
        ("num", SimpleImputer(strategy="median"), numeric_features),

        ("cat", Pipeline([
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("encoder", OneHotEncoder(handle_unknown="ignore"))
        ]), categorical_features)
    ])

    # --- Model ---
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

    y = df["style"].map({
        "value": 0,
        "growth": 1
    })

    model.fit(X, y)

    return model

class StyleClassifier:

    def __init__(self, model):
        self.model = model

    def classify_symbols(self, symbols: list[str]):

        rows = []

        for symbol in symbols:

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
    
def get_classifier(update_training_set: bool):
    if update_training_set:
        update_factor_cache()

    model = train_model()
    return StyleClassifier(model)