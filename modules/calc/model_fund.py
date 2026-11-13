import log
import pandas as pd
from datetime import date, timedelta
from typing import Any, List, Literal, Optional, cast
from dataclasses import dataclass
import math
from pydantic import BaseModel, field_validator
from modules.core.text_format import table
from modules.object import best_idea, ticker
from modules.ticker import company, index_funds


# rank 1 = 512, 2 = 256, ... 10 = 1: each ETF's best ideas run to rank 10
# (best_ideas_generator.MAX_BEST_IDEAS_PER_FUND).
DEFAULT_RANK_POINTS = [2 ** (10 - r) for r in range(1, 11)]
# A holding is kept while it's within the top ceil(target x KEEP_BUFFER) of its bucket by score.
KEEP_BUFFER = 1.25


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
    # Points a company earns from each ETF by its rank there (index = rank - 1; ranks beyond the
    # list earn none), summed across the ETFs into its score - the selection's order. The halving
    # scale makes rank 1 worth two rank 2s, four rank 3s, ... (consensus over a single conviction).
    rank_points: list[int] = DEFAULT_RANK_POINTS
    # Which best ideas the fund draws on (best_idea.benchmark_mode): 'full_universe' (its region's blend
    # benchmark), 'full_value_growth_split' (each company against its own style's benchmark — US value /
    # growth — else the blend) or 'self' (the ETF's own holdings, market-cap weighted).
    benchmark: Literal['full_universe', 'full_value_growth_split', 'self'] = 'full_universe'
    recalc_frequency_days: int = 7

    @field_validator('rank_points')
    @classmethod
    def _points_decrease(cls, v: list[int]) -> list[int]:
        if not v or any(p <= 0 for p in v) or any(a < b for a, b in zip(v, v[1:])):
            raise ValueError(f"rank_points must be positive and non-increasing: {v}")
        return v

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
    ranking: int                    # the company's best rank in any ETF on the holding date
    source_etf_id: int
    max_delta: float | None
    weight: float | None = None
    score: int | None = None        # its consensus score (Strategy.rank_points) on the holding date


@dataclass
class FundHoldingChange:
    fund_id: int
    ticker_id: int
    change_date: date
    direction: str
    ranking: int | None = None
    score: int | None = None
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
    """The fund's buys and sells, then its holdings, each a table (text_format.table; the cron
    email renders them as HTML tables), blocks separated by a blank line."""
    all_ids: list[int] = list({
        *{h.ticker_id for h in results.holdings},
        *{ch.ticker_id for ch in results.changes},
    })
    tickers = ticker.fetch_by_ids(all_ids)
    ticker_by_id = {t.id: t for t in tickers}

    def symbol_name(ticker_id: int) -> list[str]:
        t = ticker_by_id.get(ticker_id)
        return [t.symbol, t.name] if t else [str(ticker_id), "-"]

    blocks: list[str] = [results.fund.name] if include_header else []

    for direction in ('buy', 'sell'):
        changes = [ch for ch in results.changes if ch.direction == direction]
        if not changes:
            blocks.append(f"{direction.capitalize()}: none")
            continue
        blocks.append(f"{direction.capitalize()} ({len(changes)})\n" + table(
            ["Score", "Rank", "Appearances", "Symbol", "Name"],
            [[ch.score or "-", ch.ranking or "-", ch.appearances or "-", *symbol_name(ch.ticker_id)] for ch in changes],
        ))

    if include_holdings:
        # Weight, then the selection's order (score, then delta): with equal weights the weight
        # alone leaves the holdings in storage order.
        holdings = sorted(results.holdings, key=lambda h: (-(h.weight or 0), -(h.score or 0), -(h.max_delta or 0)))
        blocks.append(f"Holdings ({len(holdings)})\n" + table(
            ["Percent", "Symbol", "Name"],
            [[f"{h.weight * 100:.2f}%" if h.weight is not None else "-", *symbol_name(h.ticker_id)] for h in holdings],
        ))

    return "\n\n".join(blocks)


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


def fund_best_ideas(all_best_ideas_df: pd.DataFrame, strategy: Strategy, as_of_date: date) -> pd.DataFrame:
    """
    The best ideas a fund draws on: its benchmark mode, from each ETF's latest date within the
    fund's look-back (recalc_frequency_days, inclusive). Every ETF contributes one set of best
    ideas and all count equally - a newer file doesn't outrank another ETF's view of a company.
    """
    df = all_best_ideas_df[all_best_ideas_df['benchmark_mode'] == strategy.benchmark]
    df = df[df['value_date'] >= as_of_date - timedelta(days=strategy.recalc_frequency_days)]
    return df[df['value_date'] == df.groupby('provider_etf_id')['value_date'].transform('max')]


def large_cap_mask(df: pd.DataFrame) -> pd.Series:
    """Rows whose company is large cap: its whole cap at or above its region's cutoff
    (large_cap_min) with at least index_funds.MIN_FLOAT_FACTOR floating (float_factor; unknown
    passes) - index_funds.passes_large, as the benchmarks apply it. Both columns are added by
    funds_update.build_shared_context."""
    float_ok = df['float_factor'].isna() | (pd.to_numeric(df['float_factor'], errors='coerce') >= index_funds.MIN_FLOAT_FACTOR)
    return (pd.to_numeric(df['market_cap'], errors='coerce') >= pd.to_numeric(df['large_cap_min'], errors='coerce')) & float_ok


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
        # A stock that has dropped below the large-cap cutoff since it was bought is still
        # let through here if it's already one of this fund's current holdings (held_ticker_ids),
        # so it stays eligible to be ranked/kept rather than being force-sold purely for falling
        # below the cutoff — it will still be sold once its ranking genuinely drops out. This
        # only ever widens the pool for a fund that already holds the ticker; funds that don't
        # already hold it still can't pick it up as a fresh buy. See the matching comment in
        # best_ideas_generator.compute_active_weights for why the full_universe benchmark_weight
        # defaults to 0.0 for these instead of dropping the row outright.
        masks['cap'] = large_cap_mask(df) | df[key].isin(held_ticker_ids or set())
    elif cap_type == 'mid_small':
        # A company with no cap (or no cutoff for its region) is neither large nor mid/small.
        known = pd.to_numeric(df['market_cap'], errors='coerce').notna() & pd.to_numeric(df['large_cap_min'], errors='coerce').notna()
        masks['cap'] = known & ~large_cap_mask(df)
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
    rank_points: list[int],
    exclude_ticker_ids: set[int] | None = None,
    held_ticker_ids: set[int] | None = None,
) -> pd.DataFrame:
    """
    Filter the fund's best-ideas DataFrame (fund_best_ideas: one date per ETF) for a specific
    fund configuration and aggregate per company: its score (the rank_points each ETF gives it,
    summed), best rank, appearances (the ETFs scoring it), max delta and source ETF (the ETF
    with that delta). Companies scoring 0 are left out; the rest are ordered by score, then
    appearances, then max delta - the selection's order.
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

    empty = pd.DataFrame(columns=['ticker_id', 'score', 'ranking', 'appearances', 'max_delta', 'source_etf_id', 'all_provider_ids'])
    if filtered.empty:
        return empty

    # Each ETF scores a company once, at its best rank there (two listings of one company)
    filtered = filtered.loc[filtered.groupby([key, 'provider_etf_id'])['ranking'].idxmin()]
    filtered['points'] = rank_points_of(filtered['ranking'], rank_points)
    filtered = filtered[filtered['points'] > 0]
    if filtered.empty:
        return empty

    # source_etf_id = ETF with highest delta per company
    source_idx = filtered.groupby(key)['delta'].idxmax()
    source_etf = filtered.loc[source_idx].set_index(key)['provider_etf_id']

    agg = filtered.groupby(key).agg(
        score=('points', 'sum'),
        ranking=('ranking', 'min'),
        appearances=('provider_etf_id', 'nunique'),
        max_delta=('delta', 'max'),
        all_provider_ids=('provider_etf_id', lambda s: sorted(set(s))),
    ).reset_index()

    agg['source_etf_id'] = agg[key].map(source_etf).astype(int)

    return (
        agg.rename(columns={key: 'ticker_id'})
           .sort_values(['score', 'appearances', 'max_delta'], ascending=[False, False, False])
           .reset_index(drop=True)
    )


def rank_points_of(ranking: pd.Series, rank_points: list[int]) -> pd.Series:
    """The points each best-idea row's rank earns (rank_points[rank - 1]; 0 beyond the list)."""
    return ranking.map(lambda r: rank_points[int(r) - 1] if 1 <= int(r) <= len(rank_points) else 0)


def _df_to_ranked(df: pd.DataFrame) -> list[best_idea.BestIdeaRanked]:
    return [
        best_idea.BestIdeaRanked(
            ticker_id=int(row['ticker_id']),
            score=int(row['score']),
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
) -> tuple[list, list, set[int]]:
    """
    Returns (ideal, fetched, keep_ids), each list in score order.
    `ideal`    — the top n_holdings by score, respecting the style blend split.
    `fetched`  — every scored idea.
    `keep_ids` — the ideas within the top ceil(target x KEEP_BUFFER) of their list (per style
                 with a blend split, against that style's count): a holding among them is kept.
    """
    etf_ids = strategy.provider_etfs or []
    exchanges = strategy.exchanges or []

    def scored(style: str) -> list[best_idea.BestIdeaRanked]:
        return _df_to_ranked(_filter_and_aggregate(
            all_best_ideas_df, etf_ids, style, strategy.cap.name,
            country_type, exchanges, strategy.esg_only, strategy.rank_points, exclude_ticker_ids,
            held_ticker_ids,
        ))

    def keep(ideas: list[best_idea.BestIdeaRanked], target: int) -> set[int]:
        return {i.ticker_id for i in ideas[:math.ceil(target * KEEP_BUFFER)]}

    if (
        strategy.style.name == "blend"
        and strategy.style.value is not None
        and strategy.style.growth is not None
    ):
        fetched_growth = scored('growth')
        fetched_value = scored('value')
        growth_target = round(n_holdings * strategy.style.growth / 100)
        value_target = n_holdings - growth_target

        fetched = fetched_growth + fetched_value
        growth_count = min(len(fetched_growth), growth_target)
        value_count  = min(len(fetched_value),  n_holdings - growth_count)
        ideal = fetched_growth[:growth_count] + fetched_value[:value_count]
        keep_ids = keep(fetched_growth, growth_target) | keep(fetched_value, value_target)
    else:
        fetched = scored(strategy.style.name)
        ideal = fetched[:n_holdings]
        keep_ids = keep(fetched, n_holdings)

    return ideal, fetched, keep_ids


@dataclass
class RegionBucket:
    """One region's share of a fund: its ideal holdings (in score order, at most `target`), the
    ids of every idea scored for it, the ids a holding is kept at (within the keep buffer), and
    its number of holdings."""
    ideal: list
    fetched_ids: set[int]
    keep_ids: set[int]
    target: int


def _select_region_buckets(
    strategy: Strategy,
    all_best_ideas_df: pd.DataFrame,
    held_ticker_ids: set[int] | None = None,
) -> tuple[list[RegionBucket], list]:
    """
    Returns (buckets, fetched).
    - Split region (US + International both set): one bucket per region, each filtered
      independently, with its percentage of the holdings as its target.
    - Name-only region ("US" or "International"): one bucket, filtered to that region.
    - No region or unrecognised name: one bucket, no region filter.
    A region's buckets are disjoint: the US one excludes every company fetched for International.
    """
    region_split = strategy.region.split if strategy.region is not None else None

    if (
        region_split is not None
        and region_split.US is not None
        and region_split.International is not None
    ):
        us_n_target   = round(strategy.holdings * region_split.US / 100)
        intl_n_target = strategy.holdings - us_n_target

        intl_ideal, intl_fetched, intl_keep = _fetch_and_select_by_style(intl_n_target, strategy, all_best_ideas_df, country_type=company.INTERNATIONAL, held_ticker_ids=held_ticker_ids)
        intl_ticker_ids = {i.ticker_id for i in intl_fetched}
        us_ideal,   us_fetched,   us_keep   = _fetch_and_select_by_style(us_n_target, strategy, all_best_ideas_df, country_type=company.US, exclude_ticker_ids=intl_ticker_ids, held_ticker_ids=held_ticker_ids)

        return [
            RegionBucket(us_ideal, {i.ticker_id for i in us_fetched}, us_keep, us_n_target),
            RegionBucket(intl_ideal, intl_ticker_ids, intl_keep, intl_n_target),
        ], us_fetched + intl_fetched

    country_type = strategy_country_types(strategy)[0]
    ideal, fetched, keep_ids = _fetch_and_select_by_style(strategy.holdings, strategy, all_best_ideas_df, country_type=country_type, held_ticker_ids=held_ticker_ids)
    return [RegionBucket(ideal, {i.ticker_id for i in fetched}, keep_ids, strategy.holdings)], fetched


def strategy_country_types(strategy: Strategy) -> list[str]:
    """
    The country_type buckets _select_region_buckets selects from: both regions for a
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

    # Pass this fund's current holdings through the 'large' cap filter (see the comment in
    # _eligibility_masks) so one dropping below the large-cap threshold isn't force-sold
    # for that reason alone — it's still evaluated on its score like any other candidate.
    held_ticker_ids = {ph.ticker_id for ph in previous_holdings}
    buckets, fetched = _select_region_buckets(strategy, all_best_ideas_df, held_ticker_ids)
    ideal_holdings = [i for b in buckets for i in b.ideal]
    keep_ids = {t for b in buckets for t in b.keep_ids}
    fetched_by_id = {i.ticker_id: i for i in fetched}

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
        # A holding is kept while it's within its bucket's keep buffer (the top
        # ceil(target x KEEP_BUFFER) by score), else sold. Kept holdings carry today's score.
        for ph in previous_holdings:
            now = fetched_by_id.get(ph.ticker_id)
            if now is None or ph.ticker_id not in keep_ids:
                holdings_changed.append(FundHoldingChange(
                    fund_id=fund.id, ticker_id=ph.ticker_id, change_date=today, direction="sell",
                    score=now.score if now else None, ranking=now.ranking if now else None,
                    appearances=now.appearances if now else None,
                    reason="No longer a best idea" if now is None else "Below the score buffer",
                ))
                continue

            ph.holding_date = today
            ph.score, ph.ranking, ph.max_delta, ph.source_etf_id = now.score, now.ranking, now.max_delta, now.source_etf_id
            todays_holdings.append(ph)

        # Free places are filled region by region: each region's from its own ideal holdings, up
        # to its target less the holdings it kept (every kept holding is among its region's
        # scored ideas). A sold International holding is replaced by an International idea, not by
        # the next US one; a region short of ideas isn't made up from the other. Kept holdings are
        # never sold to make room for a higher score.
        missing = strategy.holdings - len(todays_holdings)
        existing_ids = {th.ticker_id for th in todays_holdings}
        for bucket in buckets:
            room = min(missing, bucket.target - sum(1 for t in existing_ids if t in bucket.fetched_ids))
            for fi in bucket.ideal:
                if room <= 0:
                    break
                if fi.ticker_id in existing_ids:
                    continue
                todays_holdings.append(FundHolding(
                    fund_id=fund.id, holding_date=today, ticker_id=fi.ticker_id,
                    ranking=fi.ranking, source_etf_id=fi.source_etf_id, max_delta=fi.max_delta,
                    score=fi.score,
                ))
                holdings_changed.append(FundHoldingChange(
                    fund_id=fund.id, ticker_id=fi.ticker_id, change_date=today, direction="buy",
                    ranking=fi.ranking, score=fi.score, appearances=fi.appearances, max_delta=fi.max_delta,
                    top_delta_provider_etf_id=fi.source_etf_id, all_provider_etf_ids=fi.all_provider_ids,
                ))
                existing_ids.add(fi.ticker_id)
                room -= 1
                missing -= 1

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
