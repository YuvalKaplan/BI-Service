import log
import pandas as pd
from datetime import date
from typing import Any, List, Optional, cast
from dataclasses import dataclass
from pydantic import BaseModel
from modules.const import LARGE_CAP_THRESHOLD
from modules.object import best_idea, ticker
from modules.ticker import company


# ── Shared strategy models ──────────────────────────────────────────────────

class Cap(BaseModel):
    name: str

class Style(BaseModel):
    name: str
    value: Optional[int] = None
    growth: Optional[int] = None

class RegionSplit(BaseModel):
    # Percent of the fund's holdings per region, keyed by ticker.region value. Unknown keys are
    # rejected so a misspelt split can't silently turn into "no region filter".
    model_config = {'extra': 'forbid'}
    US: Optional[int] = None
    International: Optional[int] = None

class Region(BaseModel):
    name: str
    split: Optional[RegionSplit] = None

class Strategy(BaseModel):
    allocation: str
    allocation_rebalance: str = 'on_change'  # 'none' | 'on_change' | 'full'
    holdings: int
    cap: Cap
    style: Style
    region: Optional[Region] = None
    thematic: Optional[str] = None
    benchmarks: Optional[list[str]] = None
    provider_etfs: Optional[list[int]] = None
    exchanges: Optional[list[str]] = None
    esg_only: bool = False
    ranking_from: int = 1
    ranking_to: int = 1
    benchmark: str = 'full_universe'  # 'full_universe' | 'self'
    recalc_frequency_days: int = 7

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

MC_WEIGHT_ALPHA = 0.5   # power-law exponent: 1.0 = pure market-cap, 0.0 = equal-weight
MC_WEIGHT_CAP   = 0.10  # maximum weight per holding
MC_WEIGHT_FLOOR = 0.01  # minimum weight per holding


# ── Helpers ──────────────────────────────────────────────────────────────────

def results_to_string(results: FundChangesResult, include_header: bool = True, include_holdings: bool = True) -> str:
    aggregator = ""
    all_ids: list[int] = list({
        *{h.ticker_id for h in results.holdings},
        *{ch.ticker_id for ch in results.changes},
    })
    tickers = ticker.fetch_by_ids(all_ids)
    ticker_by_id = {t.id: t for t in tickers}

    if include_header:
        aggregator += f"{results.fund.name}\n" + "=" * 20 + "\n"

    if include_holdings:
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
    Adds a `canonical_ticker_id` column = COALESCE(master_ticker_id, ticker_id). Share-class/
    cross-listing grouping is no longer computed here — it's done once, persistently, by
    modules.ticker.master.sync_master_tickers() (CIK match, falling back to normalized-name
    match for tickers with no CIK, plus modules.ticker.identity evidence) and stored on
    ticker.master_ticker_id — the company's primary listing, which market-cap moves never change. Tickers with no master keep canonical_ticker_id == ticker_id.
    """
    if df.empty or 'master_ticker_id' not in df.columns:
        return df.assign(canonical_ticker_id=df['ticker_id'])
    # A mixed None/int DB column comes back from pandas as float64 with NaN standing in for
    # None — and NaN is truthy in Python, so `m or t` would wrongly keep NaN instead of falling
    # back to t. pd.isna() is the dtype-safe way to detect "no master" here (this also sidesteps
    # pandas' fillna-downcast FutureWarning on a mixed-dtype object column).
    canonical = [t if pd.isna(m) else m for m, t in zip(df['master_ticker_id'], df['ticker_id'])]
    return df.assign(canonical_ticker_id=pd.array(canonical, dtype='int64'))


def _eligibility_masks(
    df: pd.DataFrame,
    provider_etf_ids: list[int],
    style_type: str | None,
    cap_type: str,
    country_type: str,
    exchanges: list[str],
    esg_only: bool,
    held_ticker_ids: set[int] | None = None,
) -> dict[str, pd.Series]:
    """
    The fund-configuration filters applied to the best-ideas DataFrame, one named boolean mask
    per criterion (a row is eligible when all are True). Kept separate so the methodology
    report can say which filter excluded a row, using exactly the logic the selection uses.
    """
    key = 'canonical_ticker_id'
    etf_set = set(provider_etf_ids)
    all_true = pd.Series(True, index=df.index)
    masks: dict[str, pd.Series] = {}

    masks['etf'] = df['provider_etf_id'].isin(etf_set) if etf_set else all_true
    masks['style'] = df['style_type'] == style_type if style_type not in (None, 'core', 'blend') else all_true
    if cap_type == 'large':
        # A stock that has dropped below the large-cap threshold since it was bought is still
        # let through here if it's already one of this fund's current holdings (held_ticker_ids),
        # so it stays eligible to be ranked/kept rather than being force-sold purely for falling
        # below the threshold — it will still be sold once its ranking genuinely drops out. This
        # only ever widens the pool for a fund that already holds the ticker; funds that don't
        # already hold it still can't pick it up as a fresh buy. See the matching comment in
        # best_ideas_generator.compute_active_weights for why the full_universe benchmark_weight
        # defaults to 0.0 for these instead of dropping the row outright.
        masks['cap'] = (df['market_cap'] >= LARGE_CAP_THRESHOLD) | df[key].isin(held_ticker_ids or set())
    elif cap_type == 'mid_small':
        masks['cap'] = df['market_cap'] < LARGE_CAP_THRESHOLD
    else:
        masks['cap'] = all_true
    # country_type is a region ('US' | 'International') or 'all'. A company's region is its
    # primary listing's (ticker.region, see modules/ticker/company.py): US-listed
    # foreign-domiciled companies (Eaton, Medtronic) are US; ADRs of companies with a home-market
    # listing (TSM, ASML) are International — the same rule the benchmarks use.
    if country_type == 'all':
        masks['region'] = all_true
    else:
        masks['region'] = (df['etf_region'] == country_type) & (df['region'] == country_type)
    masks['exchange'] = df['exchange'].isin(exchanges) if exchanges else all_true
    masks['esg'] = df['esg_qualified'] == True if esg_only else all_true
    return masks


def _filter_and_aggregate(
    df: pd.DataFrame,
    provider_etf_ids: list[int],
    style_type: str,
    cap_type: str,
    country_type: str,
    exchanges: list[str],
    esg_only: bool,
    ranking_level: int,
    exclude_ticker_ids: set[int] | None = None,
    held_ticker_ids: set[int] | None = None,
) -> pd.DataFrame:
    """
    Filter the global best-ideas DataFrame for a specific fund configuration
    and aggregate per ticker, preserving the same semantics as the SQL function:
    latest date per ticker, best ranking on that date, appearances/max_delta/
    source_etf_id all relative to this fund's ETF list.
    """
    key = 'canonical_ticker_id'

    mask = pd.Series(True, index=df.index)
    for m in _eligibility_masks(df, provider_etf_ids, style_type, cap_type, country_type, exchanges, esg_only, held_ticker_ids).values():
        mask &= m

    filtered = df[mask].copy()
    if exclude_ticker_ids:
        filtered = filtered[~filtered[key].isin(exclude_ticker_ids)]

    # Printout full list to CSV for debugging
    # filtered.sort_values(['provider_etf_id', 'ranking']).to_csv('debug_filtered.csv', index=False)

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
    exclude_ticker_ids: set[int] | None = None,
    held_ticker_ids: set[int] | None = None,
) -> tuple[list, list]:
    """
    Returns (ideal, fetched).
    `ideal`   — top-ranked ideas capped at n_holdings, respecting style blend split.
    `fetched` — all retrieved ideas (used by caller for ranking-drop detection).
    """
    etf_ids = strategy.provider_etfs or []
    exchanges = strategy.exchanges or []
    ranking_level = strategy.ranking_to + (5 if strategy.allocation == 'market_cap' else 2)

    if (
        strategy.style.name == "blend"
        and strategy.style.value is not None
        and strategy.style.growth is not None
    ):
        fetched_growth = _df_to_ranked(_filter_and_aggregate(
            all_best_ideas_df, etf_ids, 'growth', strategy.cap.name,
            country_type, exchanges, strategy.esg_only, ranking_level, exclude_ticker_ids,
            held_ticker_ids,
        ))
        fetched_value = _df_to_ranked(_filter_and_aggregate(
            all_best_ideas_df, etf_ids, 'value', strategy.cap.name,
            country_type, exchanges, strategy.esg_only, ranking_level, exclude_ticker_ids,
            held_ticker_ids,
        ))

        if strategy.ranking_from != 1:
            fetched_growth = [i for i in fetched_growth if strategy.ranking_from <= i.ranking]
            fetched_value  = [i for i in fetched_value  if strategy.ranking_from <= i.ranking]

        fetched = fetched_growth + fetched_value
        growth_in_range = [i for i in fetched_growth if i.ranking <= strategy.ranking_to]
        value_in_range  = [i for i in fetched_value  if i.ranking <= strategy.ranking_to]

        growth_count = min(len(growth_in_range), round(n_holdings * strategy.style.growth / 100))
        value_count  = min(len(value_in_range),  n_holdings - growth_count)
        ideal = growth_in_range[:growth_count] + value_in_range[:value_count]
    else:
        fetched = _df_to_ranked(_filter_and_aggregate(
            all_best_ideas_df, etf_ids, strategy.style.name, strategy.cap.name,
            country_type, exchanges, strategy.esg_only, ranking_level, exclude_ticker_ids,
            held_ticker_ids,
        ))

        if strategy.ranking_from != 1:
            fetched = [i for i in fetched if strategy.ranking_from <= i.ranking]

        in_range = [i for i in fetched if i.ranking <= strategy.ranking_to]
        ideal = in_range[:n_holdings]

    return ideal, fetched


def _fetch_and_select_by_region(
    strategy: Strategy,
    all_best_ideas_df: pd.DataFrame,
    held_ticker_ids: set[int] | None = None,
) -> tuple[list, list]:
    """
    Returns (ideal, fetched).
    - Split region (US + International both set): filters each region independently then combines by percentages.
    - Name-only region ("US" or "International"): filters all holdings to that region.
    - No region or unrecognised name: no region filter applied.
    """
    region_split = strategy.region.split if strategy.region is not None else None

    if (
        region_split is not None
        and region_split.US is not None
        and region_split.International is not None
    ):
        intl_ideal, intl_fetched = _fetch_and_select_by_style(strategy.holdings, strategy, all_best_ideas_df, country_type=company.INTERNATIONAL, held_ticker_ids=held_ticker_ids)
        intl_ticker_ids = {i.ticker_id for i in intl_fetched}
        us_ideal,   us_fetched   = _fetch_and_select_by_style(strategy.holdings, strategy, all_best_ideas_df, country_type=company.US, exclude_ticker_ids=intl_ticker_ids, held_ticker_ids=held_ticker_ids)

        us_n_target   = round(strategy.holdings * region_split.US / 100)
        intl_n_target = strategy.holdings - us_n_target
        us_n   = min(len(us_ideal),   us_n_target)
        intl_n = min(len(intl_ideal), intl_n_target)

        return us_ideal[:us_n] + intl_ideal[:intl_n], us_fetched + intl_fetched

    country_type = strategy_country_types(strategy)[0]
    return _fetch_and_select_by_style(strategy.holdings, strategy, all_best_ideas_df, country_type=country_type, held_ticker_ids=held_ticker_ids)


def strategy_country_types(strategy: Strategy) -> list[str]:
    """
    The country_type buckets _fetch_and_select_by_region selects from: both regions for a
    US/International split, the region's name when it's one of them, else ['all'] (no filter).
    """
    region = strategy.region
    if region is None:
        return ['all']
    split = region.split
    if split is not None and split.US is not None and split.International is not None:
        return [company.US, company.INTERNATIONAL]
    if region.name in (company.US, company.INTERNATIONAL):
        return [region.name]
    return ['all']


def etfs_used(strategy: Strategy, all_best_ideas_df: pd.DataFrame) -> dict[int, date]:
    """
    {provider_etf_id: value_date} of the ETFs whose best ideas this fund's selection can draw
    on: the strategy's ETF list (all ETFs when empty), limited to the ETF regions its country
    buckets accept (same ETF-level conditions as _eligibility_masks).
    """
    df = all_best_ideas_df
    if strategy.provider_etfs:
        df = df[df['provider_etf_id'].isin(set(strategy.provider_etfs))]
    country_types = strategy_country_types(strategy)
    if 'all' not in country_types:
        df = df[df['etf_region'].isin(country_types)]
    # pandas types to_dict() as dict[Hashable, Any]; the keys are provider_etf_id ints.
    return cast(dict[int, date], df.groupby('provider_etf_id')['value_date'].max().to_dict())


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

    # Pass this fund's current holdings through the 'large' cap filter (see the comment in
    # _filter_and_aggregate) so one dropping below the large-cap threshold isn't force-sold
    # for that reason alone — it's still evaluated on ranking like any other candidate.
    held_ticker_ids = {ph.ticker_id for ph in previous_holdings}
    ideal_holdings, fetched = _fetch_and_select_by_region(strategy, all_best_ideas_df, held_ticker_ids)

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
