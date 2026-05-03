import log
import pandas as pd
from datetime import date
from typing import Any, List, Optional
from dataclasses import dataclass
from pydantic import BaseModel, Field
from modules.object import best_idea, ticker


# ── Shared strategy models ──────────────────────────────────────────────────

class Cap(BaseModel):
    name: str

class Style(BaseModel):
    name: str
    value: Optional[int] = None
    growth: Optional[int] = None

class RegionSplit(BaseModel):
    model_config = {'populate_by_name': True}
    us: Optional[int] = Field(None, alias='US')
    non_us: Optional[int] = Field(None, alias='Non-US')

class Region(BaseModel):
    name: str
    split: Optional[RegionSplit] = None

class Strategy(BaseModel):
    allocation: str
    holdings: int
    cap: Cap
    style: Style
    region: Optional[Region] = None
    thematic: Optional[str] = None
    benchmarks: Optional[list[str]] = None
    provider_etfs: Optional[list[int]] = None
    exchanges: Optional[list[str]] = None
    esg_only: bool = False

def getStrategyFromJson(data: dict) -> Strategy:
    return Strategy.model_validate(data)


# ── Shared dataclasses ───────────────────────────────────────────────────────

@dataclass
class FundProtocol:
    id: int
    name: str
    strategy: dict

def to_fund_protocol(f: Any) -> FundProtocol:
    """Convert any Fund dataclass (live or BT) to a FundProtocol."""
    return FundProtocol(id=f.id, name=f.name, strategy=f.strategy)


@dataclass
class FundHolding:
    fund_id: int
    ticker_id: int
    holding_date: date
    ranking: int
    source_etf_id: int
    max_delta: float | None
    weight: float | None = None


@dataclass
class FundHoldingChange:
    fund_id: int
    ticker_id: int
    change_date: date
    direction: str
    ranking: int | None = None
    appearances: int | None = None
    max_delta: float | None = None
    top_delta_provider_etf_id: int | None = None
    all_provider_etf_ids: list[int] | None = None
    reason: str | None = None


@dataclass
class FundChangesResult:
    fund: FundProtocol
    holdings: List[FundHolding]
    changes: List[FundHoldingChange]


# ── Constants ────────────────────────────────────────────────────────────────

USE_RANKING_HIGH = 1
USE_RANKING_LOW = 1

MC_WEIGHT_ALPHA = 0.5   # power-law exponent: 1.0 = pure market-cap, 0.0 = equal-weight
MC_WEIGHT_CAP   = 0.10  # maximum weight per holding
MC_WEIGHT_FLOOR = 0.01  # minimum weight per holding


# ── Helpers ──────────────────────────────────────────────────────────────────

def results_to_string(results: FundChangesResult) -> str:
    aggregator = ""
    all_ids: list[int] = list({
        *{h.ticker_id for h in results.holdings},
        *{ch.ticker_id for ch in results.changes},
    })
    tickers = ticker.fetch_by_ids(all_ids)
    ticker_by_id = {t.id: t for t in tickers}

    aggregator += f"{results.fund.name}\n" + "=" * 20 + "\n"

    aggregator += f"Holdings ({len(results.holdings)}):\n"
    aggregator += "{:<12}{:<35}{}\n".format("Symbol", "Name", "Weight")
    for h in sorted(results.holdings, key=lambda h: h.weight or 0, reverse=True):
        t = ticker_by_id.get(h.ticker_id)
        weight_str = f"{h.weight * 100:.2f}%" if h.weight is not None else "---"
        aggregator += "{:<12}{:<35}{}\n".format(
            t.symbol if t else str(h.ticker_id),
            t.name if t else "---",
            weight_str,
        )
    aggregator += "\n"

    if not results.changes:
        aggregator += "No changes\n\n"
    else:
        aggregator += "{:<12}{:<15}{:<12}{:<15}{:<10}{}\n".format(
            "Direction", "Date", "Ranking", "Appearances", "Symbol", "Name"
        )
        for ch in results.changes:
            t = ticker_by_id.get(ch.ticker_id)
            date_str = ch.change_date.strftime("%Y-%m-%d") if ch.change_date else "---"
            aggregator += "{:<12}{:<15}{:<12}{:<15}{:<10}{}\n".format(
                ch.direction,
                date_str,
                ch.ranking if ch.ranking else "---",
                ch.appearances if ch.appearances else "---",
                t.symbol if t else str(ch.ticker_id),
                t.name if t else "---",
            )
        aggregator += "-" * 30 + "\n\n"

    return aggregator


# ── Core computation ─────────────────────────────────────────────────────────

def resolve_canonical_ticker_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a `canonical_ticker_id` column. All cross-listings of the same company
    (matched by normalised name) are mapped to one canonical ticker_id.
    Selection priority: highest market_cap > US-listed > lowest ticker_id.
    Rows with no name keep canonical_ticker_id == ticker_id.
    """
    if df.empty or 'name' not in df.columns:
        return df.assign(canonical_ticker_id=df['ticker_id'])

    norm_name = df['name'].str.strip().str.lower()
    has_name  = norm_name.notna() & (norm_name != '')

    ticker_attrs = (
        df[has_name][['ticker_id', 'country', 'market_cap']]
        .assign(_norm=norm_name[has_name])
        .drop_duplicates(subset='ticker_id')
    )

    def pick_canonical(group):
        group = group.copy()
        group['_is_us'] = (group['country'] == 'US').astype(int)
        return group.sort_values(
            ['market_cap', '_is_us', 'ticker_id'],
            ascending=[False, False, True],
        )['ticker_id'].iloc[0]

    canonical_map = ticker_attrs.groupby('_norm').apply(pick_canonical)

    df = df.copy()
    df['canonical_ticker_id'] = df['ticker_id']
    df.loc[has_name, 'canonical_ticker_id'] = norm_name[has_name].map(canonical_map).astype(int)
    return df


def _filter_and_aggregate(
    df: pd.DataFrame,
    provider_etf_ids: list[int],
    style_type: str,
    cap_type: str,
    country_type: str,
    exchanges: list[str],
    esg_only: bool,
    ranking_level: int,
) -> pd.DataFrame:
    """
    Filter the global best-ideas DataFrame for a specific fund configuration
    and aggregate per ticker, preserving the same semantics as the SQL function:
    latest date per ticker, best ranking on that date, appearances/max_delta/
    source_etf_id all relative to this fund's ETF list.
    """
    key = 'canonical_ticker_id'
    etf_set = set(provider_etf_ids)

    CAP_LARGE_THRESHOLD = 10_000_000_000

    mask = pd.Series(True, index=df.index)
    if etf_set:
        mask &= df['provider_etf_id'].isin(etf_set)
    if style_type != 'blend':
        mask &= df['style_type'] == style_type
    if cap_type == 'large':
        mask &= df['market_cap'] >= CAP_LARGE_THRESHOLD
    elif cap_type == 'mid_small':
        mask &= df['market_cap'] < CAP_LARGE_THRESHOLD
    if country_type == 'US':
        mask &= df['country'] == 'US'
    elif country_type == 'Non-US':
        mask &= df['country'] != 'US'
    if exchanges:
        mask &= df['exchange'].isin(exchanges)
    if esg_only:
        mask &= df['esg_qualified'] == True

    filtered = df[mask].copy()

    empty = pd.DataFrame(columns=['ticker_id', 'ranking', 'appearances', 'max_delta', 'source_etf_id', 'all_provider_ids'])
    if filtered.empty:
        return empty

    # Per company: keep only rows from its latest date (mirrors symbol_latest_date CTE)
    ticker_max_date = filtered.groupby(key)['value_date'].transform('max')
    filtered = filtered[filtered['value_date'] == ticker_max_date]

    # Per company on its latest date: keep only its best ranking (mirrors symbol_targets CTE)
    ticker_best_rank = filtered.groupby(key)['ranking'].transform('min')
    filtered = filtered[filtered['ranking'] == ticker_best_rank]

    # Cap at ranking_level
    filtered = filtered[filtered['ranking'] <= ranking_level]
    if filtered.empty:
        return empty

    # source_etf_id = ETF with highest delta per company
    source_idx = filtered.groupby(key)['delta'].idxmax()
    source_etf = filtered.loc[source_idx].set_index(key)['provider_etf_id']

    agg = filtered.groupby(key).agg(
        ranking=('ranking', 'first'),
        appearances=('provider_etf_id', 'nunique'),
        max_delta=('delta', 'max'),
        all_provider_ids=('provider_etf_id', list),
    ).reset_index()

    agg['source_etf_id'] = agg[key].map(source_etf).astype(int)

    return (
        agg.rename(columns={key: 'ticker_id'})
           .sort_values(['ranking', 'appearances', 'max_delta'], ascending=[True, False, False])
           .reset_index(drop=True)
    )


def _df_to_ranked(df: pd.DataFrame) -> list[best_idea.BestIdeaRanked]:
    return [
        best_idea.BestIdeaRanked(
            ticker_id=int(row['ticker_id']),
            ranking=int(row['ranking']),
            appearances=int(row['appearances']),
            max_delta=float(row['max_delta']),
            source_etf_id=int(row['source_etf_id']),
            all_provider_ids=list(row['all_provider_ids']),
        )
        for row in df.to_dict('records')
    ]


def _fetch_and_select_by_style(
    n_holdings: int,
    strategy: Strategy,
    all_best_ideas_df: pd.DataFrame,
    country_type: str = 'all',
) -> tuple[list, list]:
    """
    Returns (ideal, fetched).
    `ideal`   — top-ranked ideas capped at n_holdings, respecting style blend split.
    `fetched` — all retrieved ideas (used by caller for ranking-drop detection).
    """
    etf_ids = strategy.provider_etfs or []
    exchanges = strategy.exchanges or []
    ranking_level = USE_RANKING_LOW + (5 if strategy.allocation == 'market_cap' else 2)

    if (
        strategy.style.name == "blend"
        and strategy.style.value is not None
        and strategy.style.growth is not None
    ):
        fetched_growth = _df_to_ranked(_filter_and_aggregate(
            all_best_ideas_df, etf_ids, 'growth', strategy.cap.name,
            country_type, exchanges, strategy.esg_only, ranking_level,
        ))
        fetched_value = _df_to_ranked(_filter_and_aggregate(
            all_best_ideas_df, etf_ids, 'value', strategy.cap.name,
            country_type, exchanges, strategy.esg_only, ranking_level,
        ))

        if USE_RANKING_HIGH != 1:
            fetched_growth = [i for i in fetched_growth if USE_RANKING_HIGH <= i.ranking]
            fetched_value  = [i for i in fetched_value  if USE_RANKING_HIGH <= i.ranking]

        fetched = fetched_growth + fetched_value
        growth_in_range = [i for i in fetched_growth if i.ranking <= USE_RANKING_LOW]
        value_in_range  = [i for i in fetched_value  if i.ranking <= USE_RANKING_LOW]

        growth_count = min(len(growth_in_range), round(n_holdings * strategy.style.growth / 100))
        value_count  = min(len(value_in_range),  n_holdings - growth_count)
        ideal = growth_in_range[:growth_count] + value_in_range[:value_count]
    else:
        fetched = _df_to_ranked(_filter_and_aggregate(
            all_best_ideas_df, etf_ids, strategy.style.name, strategy.cap.name,
            country_type, exchanges, strategy.esg_only, ranking_level,
        ))

        if USE_RANKING_HIGH != 1:
            fetched = [i for i in fetched if USE_RANKING_HIGH <= i.ranking]

        in_range = [i for i in fetched if i.ranking <= USE_RANKING_LOW]
        ideal = in_range[:n_holdings]

    return ideal, fetched


def _fetch_and_select_by_region(
    strategy: Strategy,
    all_best_ideas_df: pd.DataFrame,
) -> tuple[list, list]:
    """
    Returns (ideal, fetched).
    - Split region (us + non_us both set): filters US and Non-US independently then combines by percentages.
    - Name-only region ("US" or "International"): filters all holdings to that country group.
    - No region or unrecognised name: no country filter applied.
    """
    region = strategy.region
    region_split = region.split if region is not None else None

    if (
        region_split is not None
        and region_split.us is not None
        and region_split.non_us is not None
    ):
        us_ideal,   us_fetched   = _fetch_and_select_by_style(strategy.holdings, strategy, all_best_ideas_df, country_type='US')
        intl_ideal, intl_fetched = _fetch_and_select_by_style(strategy.holdings, strategy, all_best_ideas_df, country_type='Non-US')

        us_n_target   = round(strategy.holdings * region_split.us / 100)
        intl_n_target = strategy.holdings - us_n_target
        us_n   = min(len(us_ideal),   us_n_target)
        intl_n = min(len(intl_ideal), intl_n_target)

        return us_ideal[:us_n] + intl_ideal[:intl_n], us_fetched + intl_fetched

    country_type = 'all'
    if region is not None:
        if region.name == 'US':
            country_type = 'US'
        elif region.name == 'International':
            country_type = 'Non-US'

    return _fetch_and_select_by_style(strategy.holdings, strategy, all_best_ideas_df, country_type=country_type)


def generate(
    today: date,
    fund: FundProtocol,
    previous_holdings: List[FundHolding],
    all_best_ideas_df: pd.DataFrame,
    mc_map: dict,
) -> FundChangesResult:
    """
    Pure computation: determine today's holdings and changes for each fund.
    No DB access — callers are responsible for fetching previous holdings
    and saving the results.
    """
    strategy = getStrategyFromJson(fund.strategy)
    ranking_gap_drop = 5 if strategy.allocation == "market_cap" else 2

    ideal_holdings, fetched = _fetch_and_select_by_region(strategy, all_best_ideas_df)

    holdings_changed: List[FundHoldingChange] = []
    todays_holdings: List[FundHolding] = []

    if not ideal_holdings:
        for ph in previous_holdings:
            ph.holding_date = today
            todays_holdings.append(ph)
        log.record_status(
            f"Ideal holdings empty for '{fund.name}'. "
            f"Carried over {len(todays_holdings)} holdings."
        )
    else:
        for ph in previous_holdings:
            found_in_ideal = next((x for x in ideal_holdings if x.ticker_id == ph.ticker_id), None)
            if found_in_ideal is None:
                found_in_fetched = next((x for x in fetched if x.ticker_id == ph.ticker_id), None)
                if found_in_fetched is None:
                    holdings_changed.append(FundHoldingChange(
                        fund_id=fund.id, ticker_id=ph.ticker_id, change_date=today,
                        direction="sell", reason="Not in best ideas top levels",
                    ))
                    continue
                if found_in_fetched.ranking - ph.ranking >= ranking_gap_drop:
                    holdings_changed.append(FundHoldingChange(
                        fund_id=fund.id, ticker_id=ph.ticker_id, change_date=today,
                        direction="sell", reason="Dropped below min ranking",
                    ))
                    continue

            ph.holding_date = today
            todays_holdings.append(ph)

        missing = strategy.holdings - len(todays_holdings)
        if missing > 0:
            existing_ids = {th.ticker_id for th in todays_holdings}
            for fi in ideal_holdings:
                if fi.ticker_id in existing_ids:
                    continue
                todays_holdings.append(FundHolding(
                    fund_id=fund.id, holding_date=today, ticker_id=fi.ticker_id,
                    ranking=fi.ranking, source_etf_id=fi.source_etf_id, max_delta=fi.max_delta,
                ))
                holdings_changed.append(FundHoldingChange(
                    fund_id=fund.id, ticker_id=fi.ticker_id, change_date=today, direction="buy",
                    ranking=fi.ranking, appearances=fi.appearances, max_delta=fi.max_delta,
                    top_delta_provider_etf_id=fi.source_etf_id, all_provider_etf_ids=fi.all_provider_ids,
                ))
                existing_ids.add(fi.ticker_id)
                missing -= 1
                if missing == 0:
                    break

    if strategy.allocation == 'market_cap':
        apply_market_cap_weights(todays_holdings, mc_map)
    else:
        apply_equal_weights(todays_holdings)

    return FundChangesResult(fund=fund, holdings=todays_holdings, changes=holdings_changed)


def apply_equal_weights(holdings: List[FundHolding]) -> None:
    n = len(holdings)
    if n == 0:
        return
    w = 1.0 / n
    for h in holdings:
        h.weight = w


def apply_market_cap_weights(
    holdings: List[FundHolding],
    market_cap_map: dict,
) -> None:
    # Pure market-cap weighting produces extreme concentration: a single mega-cap
    # can absorb 40–50% of the fund while the smallest holdings fall below 0.1%.
    # To address this we apply a two-stage approach:
    #
    # Stage 1 — Power-law compression (MC_WEIGHT_ALPHA = 0.5, i.e. square root).
    #   Instead of weighting by raw market cap, we weight by mc^alpha.  This
    #   preserves the relative ordering of holdings (larger cap still gets more
    #   weight) but compresses the ratio between the largest and smallest: a
    #   company 100× bigger than another gets only 10× the weight instead of 100×.
    #
    # Stage 2 — Single cap + floor pass with proportional redistribution.
    #   After compression, holdings that still breach the hard bounds (MC_WEIGHT_CAP
    #   and MC_WEIGHT_FLOOR) are pinned to those bounds.  The net weight freed by
    #   capping minus the weight consumed by flooring is redistributed to the
    #   unconstrained "middle" holdings proportionally to their compressed weights,
    #   so their relative ordering is maintained.
    #   Holdings with no market-cap data are excluded from all calculations and
    #   receive weight = None.

    # Stage 1: apply power-law transform and normalise
    transformed = [
        (mc ** MC_WEIGHT_ALPHA if (mc := market_cap_map.get(h.ticker_id)) else None)
        for h in holdings
    ]
    total = sum(t for t in transformed if t is not None)
    if total == 0:
        return

    weights: list[float | None] = [t / total if t is not None else None for t in transformed]

    # Stage 2: cap + floor with proportional redistribution to middle holdings
    valid   = [i for i, w in enumerate(weights) if w is not None]
    capped  = [i for i in valid if weights[i] > MC_WEIGHT_CAP]   # type: ignore[operator]
    floored = [i for i in valid if weights[i] < MC_WEIGHT_FLOOR]  # type: ignore[operator]
    middle  = [i for i in valid if MC_WEIGHT_FLOOR <= weights[i] <= MC_WEIGHT_CAP]  # type: ignore[operator]

    if capped or floored:
        # net > 0: capping freed more than flooring consumed — middle holdings grow
        # net < 0: flooring consumed more than capping freed — middle holdings shrink
        excess  = sum(weights[i] - MC_WEIGHT_CAP   for i in capped)   # type: ignore[operator]
        deficit = sum(MC_WEIGHT_FLOOR - weights[i]  for i in floored)  # type: ignore[operator]
        net = excess - deficit
        for i in capped:
            weights[i] = MC_WEIGHT_CAP
        for i in floored:
            weights[i] = MC_WEIGHT_FLOOR
        if middle:
            middle_total = sum(weights[i] for i in middle)  # type: ignore[misc]
            if middle_total > 0:
                for i in middle:
                    weights[i] += net * (weights[i] / middle_total)  # type: ignore[operator]

    for h, w in zip(holdings, weights):
        h.weight = w
