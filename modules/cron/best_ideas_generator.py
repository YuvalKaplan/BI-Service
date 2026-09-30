import log
import pandas as pd
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from functools import partial
from modules.object import batch_run, batch_run_log
from modules.object import provider, provider_etf, ticker
from modules.object.provider_etf_holding import ProviderEtfHolding, QuarantinedHolding, fetch_latest_holdings_for_etf, aggregate_holdings
from modules.object.ticker_value import fetch_latest_market_caps_within_window
from modules.object import best_idea, benchmark
from modules.object.fund_analysis import FundAnalysis
from modules.sec import etf_selection
from modules.ticker import company

DAYS_NO_MARKET_CAP = 5
MIN_HOLDINGS_WITH_PRICES_PCT = 0.95
LOOK_BACK_WINDOW = etf_selection.MAX_HOLDINGS_AGE_DAYS  # an active ETF's holdings are never older
MAX_BEST_IDEAS_PER_FUND = 10
HOLDING_DELTA_LIMIT_DROP_OFF = 0.20


@dataclass
class EtfInputs:
    """Everything the active-weight calculation needs for one ETF on one holding date."""
    etf: provider_etf.ProviderEtf
    holding_date: date
    holdings: list[ProviderEtfHolding]            # one per ticker (duplicate lines summed), priced only
    market_cap_values: list
    master_info: dict[int, tuple[int | None, float | None]]
    unpriced_ticker_ids: list[int] = field(default_factory=list)
    quarantined: list[QuarantinedHolding] = field(default_factory=list)
    total_holdings: int = 0                        # after aggregation, excluding quarantined

    @property
    def coverage_ratio(self) -> float:
        return len(self.holdings) / self.total_holdings if self.total_holdings else 0.0


def prepare_etf_inputs(pe: provider_etf.ProviderEtf, up_to_date: date | None = None) -> EtfInputs | None:
    """
    Loads an ETF's latest holdings (as of up_to_date, or now) and the market data needed to
    compute its active weights. Returns None if no holdings were downloaded within
    LOOK_BACK_WINDOW. Shared by run() and the per-fund analysis snapshot so both see exactly
    the same inputs.
    """
    raw_holdings = fetch_latest_holdings_for_etf(pe.id, LOOK_BACK_WINDOW, up_to_date=up_to_date)
    if not raw_holdings:
        return None

    # A ticker listed on several lines is summed when the lines agree on price, and
    # quarantined (dropped) when they don't — see aggregate_holdings.
    holdings, quarantined = aggregate_holdings(raw_holdings)
    # holding_date is a `timestamp` column despite the dataclass typing it as `date`.
    holding_date = raw_holdings[0].holding_date
    holding_date = holding_date.date() if isinstance(holding_date, datetime) else holding_date

    # Latest market caps within +/- DAYS_NO_MARKET_CAP of the holding date. Going a few days
    # after the holding is useful if we have a new ticker that was just added to the holdings,
    # as the first market_cap downloaded using the profile API may be a day or two after it.
    ticker_ids = [h.ticker_id for h in holdings if h.ticker_id]
    market_cap_values = fetch_latest_market_caps_within_window(ticker_ids, holding_date, DAYS_NO_MARKET_CAP)

    # Master-ticker info for held tickers, plus a second hop for any master not itself among
    # the holdings (e.g. an ETF holding only GOOG still needs the company_market_cap stored on
    # GOOGL, its master).
    master_info = ticker.fetch_master_info_by_ids(ticker_ids)
    extra_master_ids = [
        mid for mid, _cap in master_info.values()
        if mid is not None and mid not in master_info
    ]
    if extra_master_ids:
        master_info.update(ticker.fetch_master_info_by_ids(extra_master_ids))

    # A master's company cap as of the holding date — the first listing, in primary-listing
    # order, with a value near that date — rather than the stored (latest) company_market_cap,
    # so historical (sim) dates weigh multi-listing companies at that date's value like every
    # other company. Masters with no listing value near the date keep the stored cap.
    master_ids = [tid for tid, (mid, cap) in master_info.items() if mid is None and cap is not None]
    for mid, cap in company.company_caps_as_of(master_ids, holding_date, DAYS_NO_MARKET_CAP).items():
        master_info[mid] = (None, cap)

    priced_ids = {v.ticker_id for v in market_cap_values}
    return EtfInputs(
        etf=pe,
        holding_date=holding_date,
        holdings=[h for h in holdings if h.ticker_id in priced_ids],
        market_cap_values=market_cap_values,
        master_info=master_info,
        unpriced_ticker_ids=[h.ticker_id for h in holdings if h.ticker_id and h.ticker_id not in priced_ids],
        quarantined=quarantined,
        total_holdings=len(holdings),
    )


def compute_active_weights(inputs: EtfInputs, benchmark_weights: dict[int, float] | None = None) -> pd.DataFrame:
    """
    Company-level active weights for every holding of the ETF (not only the positive ones).
    Columns: ticker_id (the ticker used — the master when share classes were consolidated),
    market_value, market_cap, master_used, etf_weight, benchmark_weight, delta.
    """
    master_info = inputs.master_info
    df_holdings = pd.DataFrame(
        {"ticker_id": h.ticker_id, "market_value": h.market_value}
        for h in inputs.holdings
        if h.ticker_id and h.market_value and h.market_value > 0
    )

    df_values = pd.DataFrame(
        {"ticker_id": v.ticker_id, "market_cap": v.market_cap}
        for v in inputs.market_cap_values
        if v.ticker_id and v.market_cap
    )

    if df_holdings.empty or df_values.empty:
        raise ValueError("No overlapping ticker_ids between holdings and market caps")
    df = df_holdings.merge(df_values, on="ticker_id", how="inner")

    if df.empty:
        raise ValueError("No overlapping ticker_ids between holdings and market caps")

    # Redirect share-class siblings / cross-listings (e.g. GOOGL/GOOG) onto their master ticker
    # so the ETF's exposure is combined per company, and the company is measured by its company
    # market cap (its primary listing's cap — FMP reports the whole company's cap on every
    # listing, so it is never summed across listings). Ticker ids are
    # always positive, so `or t` safely falls back to the ticker's own id when it has no
    # master (avoids pandas' fillna on a mixed None/int object column, which triggers a
    # downcast FutureWarning). The id columns hold no NaN; na_action='ignore' only tells the type
    # checker the lambda never receives one.
    df["effective_ticker_id"] = df["ticker_id"].map(lambda t: master_info.get(t, (None, None))[0] or t, na_action='ignore').astype(int)
    df["company_market_cap"] = df["effective_ticker_id"].map(lambda t: master_info.get(t, (None, None))[1], na_action='ignore')
    df["effective_market_cap"] = df.apply(
        lambda row: row["company_market_cap"] if pd.notna(row["company_market_cap"]) else row["market_cap"],
        axis=1,
    )
    # A master was used when a sibling was redirected onto it, or when the company market cap
    # stood in for the listing's own.
    df["master_used"] = (df["effective_ticker_id"] != df["ticker_id"]) | df["company_market_cap"].notna()

    # Company-level aggregation: sum the ETF's actual $ exposure across share classes of the
    # same company, producing one row per effective company rather than one per share class.
    grouped = (
        df.groupby("effective_ticker_id")
          .agg(
              market_value=("market_value", "sum"),
              market_cap=("effective_market_cap", "first"),
              master_used=("master_used", "any"),
          )
          .reset_index()
          .rename(columns={"effective_ticker_id": "ticker_id"})
    )

    total_etf_value = grouped["market_value"].sum()
    grouped["etf_weight"] = grouped["market_value"] / total_etf_value

    if benchmark_weights:
        # full_universe: the benchmark only covers stocks >= the large-cap threshold, so a
        # holding that has since dropped below it has no real weight to look up. Rather than
        # building a fund-specific benchmark for this rare/borderline case, we assume such a
        # stock's true weight in the large-cap benchmark would be negligible anyway and default
        # it to 0.0. This inflates its delta to its full ETF weight, which — on its own — would
        # make it look like a top idea to any fund that doesn't already hold it. That false
        # positive is prevented downstream in model_fund._filter_and_aggregate, which only lets
        # a below-threshold ticker through a 'large' cap fund's candidate pool when it's already
        # one of that specific fund's existing holdings (held_ticker_ids), so it can still be kept
        # until it genuinely falls out of ranking, without ever surfacing as a fresh buy.
        # full_universe: each company's weight in the external large-cap universe. Sibling rows
        # are deliberately absent from benchmark_holding (only the master is stored there), so
        # the lookup key here must already be the effective (master) id.
        grouped["benchmark_weight"] = grouped["ticker_id"].map(benchmark_weights).fillna(0.0)
    else:
        # self: market-cap weight within the ETF's own holdings, one row per company already.
        total_market_cap = grouped["market_cap"].sum()
        grouped["benchmark_weight"] = grouped["market_cap"] / total_market_cap

    grouped["delta"] = grouped["etf_weight"] - grouped["benchmark_weight"]
    return grouped


def select_best_ideas(weights: pd.DataFrame, limit: int | None = None) -> pd.DataFrame:
    """Picks the best ideas from compute_active_weights' output, ranked by delta (row order = rank)."""
    best_ideas = weights[weights["delta"] > 0].sort_values("delta", ascending=False)

    dropped = best_ideas[best_ideas["delta"] > HOLDING_DELTA_LIMIT_DROP_OFF]
    if not dropped.empty:
        log.record_status(f"Dropping {len(dropped)} best-idea(s) above delta limit {HOLDING_DELTA_LIMIT_DROP_OFF}: {dropped['ticker_id'].tolist()}")
    best_ideas = best_ideas[best_ideas["delta"] <= HOLDING_DELTA_LIMIT_DROP_OFF]

    if limit is not None:
        if limit <= 0:
            raise ValueError("limit must be a positive integer")
        best_ideas = best_ideas.head(limit)

    return best_ideas.reset_index(drop=True)


def _find_best_ideas(inputs: EtfInputs, limit: int | None = None, benchmark_weights: dict[int, float] | None = None) -> pd.DataFrame:
    return select_best_ideas(compute_active_weights(inputs, benchmark_weights), limit)


def get_benchmark_weights(
    benchmark_id: int,
    as_of_date: date | None,
    cache: dict[int, tuple[dict[int, float], date | None]],
) -> tuple[dict[int, float], date | None]:
    """{ticker_id: weight} and holding_date of the benchmark's latest snapshot as of as_of_date
    (or now), memoized in `cache` so each benchmark is fetched once per run. A snapshot stores
    each company under its master id at the time; a master moved to a better primary listing
    since (master.align_masters_to_primary) is resolved to the company's current master, the
    id compute_active_weights looks it up by."""
    if benchmark_id not in cache:
        bm_holdings = benchmark.fetch_latest_holdings(benchmark_id, LOOK_BACK_WINDOW, up_to_date=as_of_date)
        ids = [h.ticker_id for h in bm_holdings if h.ticker_id]
        master_of = {tid: mid for tid, (mid, _cap) in ticker.fetch_master_info_by_ids(ids).items() if mid}
        weights: dict[int, float] = {}
        for h in bm_holdings:
            if h.ticker_id:
                cid = master_of.get(h.ticker_id, h.ticker_id)
                weights[cid] = weights.get(cid, 0.0) + h.weight
        cache[benchmark_id] = (weights, bm_holdings[0].holding_date if bm_holdings else None)
    return cache[benchmark_id]


def build_analysis_rows(
    fund_id: int,
    as_of_date: date,
    inputs: EtfInputs,
    weights: pd.DataFrame,
    selected: pd.DataFrame,
    benchmark_id: int | None,
    benchmark_date: date | None,
) -> list[FundAnalysis]:
    """
    One fund_analysis row per company in `weights` (noting whether and why it did or didn't
    become a best idea), plus one per holding left out of the calculation for lack of a market
    cap or for being quarantined as an inconsistent duplicate.
    """
    rank_by_ticker = {int(t): rank for rank, t in enumerate(selected["ticker_id"], start=1)}
    make_row = partial(FundAnalysis, fund_id=fund_id, as_of_date=as_of_date, provider_etf_id=inputs.etf.id,
                       holding_date=inputs.holding_date, benchmark_id=benchmark_id, benchmark_date=benchmark_date)

    rows: list[FundAnalysis] = []
    for r in weights.to_dict("records"):
        ticker_id = int(r["ticker_id"])
        ranking = rank_by_ticker.get(ticker_id)
        if ranking is not None:
            note = 'best_idea'
        elif r["delta"] <= 0:
            note = 'delta<=0'
        elif r["delta"] > HOLDING_DELTA_LIMIT_DROP_OFF:
            note = 'delta>limit'
        else:
            note = 'beyond_top_N'
        rows.append(make_row(
            ticker_id=ticker_id, market_cap=float(r["market_cap"]), master_used=bool(r["master_used"]),
            etf_weight=float(r["etf_weight"]), benchmark_weight=float(r["benchmark_weight"]), delta=float(r["delta"]),
            ranking=ranking, note=note,
        ))

    # A left-out ticker can coincide with a company row when it's the master of a sibling that
    # was used — the company row already accounts for it, so it isn't repeated.
    seen = {r.ticker_id for r in rows}
    left_out = [(t, 'no_market_cap') for t in inputs.unpriced_ticker_ids] + [(q.ticker_id, 'quarantined') for q in inputs.quarantined]
    for ticker_id, note in left_out:
        if ticker_id in seen:
            continue
        seen.add(ticker_id)
        rows.append(make_row(
            ticker_id=ticker_id, market_cap=None, master_used=False,
            etf_weight=None, benchmark_weight=None, delta=None, ranking=None, note=note,
        ))
    return rows


def record_problem(batch_run_id: int, provider: provider.Provider, etf: provider_etf.ProviderEtf, error: str, message: str | None, problem_etfs: list[str]) -> None:
    item_info = f"[Provider: '{provider.name}' ({provider.id}), ETF: '{etf.name}' ({etf.id})]"
    record = f"{item_info}\t{error}\t{message or ''}"
    log.record_status(record)
    batch_run_log.insert(batch_run_log.BatchRunLog(batch_run_id=batch_run_id, note=record))
    problem_etfs.append(record)

def run(as_of_date: date | None = None) -> tuple[int, int, list[str]]:
    """
    Generates best ideas for every active provider ETF (status 'active' - the selection rules,
    modules/sec/etf_selection.py). When as_of_date is None (live),
    holdings and the full_universe benchmark are resolved as of "now". When given (sim),
    they're resolved as of that historical date instead — both fetches already support
    this natively via their up_to_date param.
    """
    process_name = 'best_ideas_generator' if as_of_date is None else 'sim_best_ideas_gen'
    log_prefix = '' if as_of_date is None else f'[sim {as_of_date}] '
    try:
        batch_run_id = batch_run.insert(batch_run.BatchRun(process=process_name, activation='auto'))

        etfs_by_provider: dict[int, list[provider_etf.ProviderEtf]] = defaultdict(list)
        for pe in provider_etf.fetch_active():
            etfs_by_provider[pe.provider_id].append(pe)
        providers = sorted(provider.fetch_by_ids(list(etfs_by_provider)), key=lambda p: p.name)
        log.record_status(f"{log_prefix}Running Best Ideas Generator batch job ID {batch_run_id} - will proccess {len(providers)} providers.")

        total_etfs = 0
        generated_etfs = 0
        problem_etfs: list[str] = []

        # {benchmark_id: ({ticker_id: weight}, holding_date)}, fetched once per unique benchmark_id.
        _bm_cache: dict[int, tuple[dict[int, float], date | None]] = {}

        for p in providers:
            pe_list = etfs_by_provider[p.id]
            total_etfs += len(pe_list)
            log.record_status(f"Starting processing provider {p.name} with {len(pe_list)} ETFs")

            for pe in pe_list:
                try:
                    # 1. Holdings for the latest holding date within the look-back window, with
                    #    duplicate lines summed or quarantined, plus market caps and master info.
                    inputs = prepare_etf_inputs(pe, as_of_date)
                    if inputs is None:
                        record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error=f"No holdings have been downloaded for the past {LOOK_BACK_WINDOW} days", message=None, problem_etfs=problem_etfs)
                        continue

                    # 2. Report quarantined duplicates (one record per ETF, not per ticker)
                    if inputs.quarantined:
                        details = "; ".join(
                            f"ticker_id={q.ticker_id} x{len(q.lines)} @ " + "/".join(f"{p_:.2f}" if p_ else "?" for p_ in q.implied_prices)
                            for q in inputs.quarantined
                        )
                        record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error="QUARANTINED DUPLICATES", message=f"{len(inputs.quarantined)} ticker(s) listed on several lines with inconsistent prices, excluded: {details}", problem_etfs=problem_etfs)

                    # 3. Identify and report stale tickers (no market cap within window)
                    for ticker_id in inputs.unpriced_ticker_ids:
                        record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error="STALE HOLDING", message=f"ticker_id={ticker_id} has no market cap for over {DAYS_NO_MARKET_CAP} days", problem_etfs=problem_etfs)

                    # 4. Coverage check
                    if inputs.coverage_ratio < MIN_HOLDINGS_WITH_PRICES_PCT:
                        missing_count = inputs.total_holdings - len(inputs.holdings)
                        msg = f"Coverage {inputs.coverage_ratio:.1%}. Missing {missing_count} market caps for holding date {inputs.holding_date.strftime('%Y-%m-%d')}"
                        record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error="Insufficient market cap coverage", message=msg, problem_etfs=problem_etfs)
                        continue

                    # 5a. Always generate self-benchmark best ideas
                    self_df = _find_best_ideas(inputs, MAX_BEST_IDEAS_PER_FUND)
                    best_idea.insert_bulk(pe.id, inputs.holding_date, 'self', best_idea.df_to_rows(self_df, provider_etf_id=pe.id, value_date=inputs.holding_date, benchmark_mode='self'))

                    # 5b. If this ETF has a benchmark configured, also generate full_universe best ideas
                    if pe.benchmark_id:
                        bm_weights, _bm_date = get_benchmark_weights(pe.benchmark_id, as_of_date, _bm_cache)
                        if bm_weights:
                            universe_df = _find_best_ideas(inputs, MAX_BEST_IDEAS_PER_FUND, benchmark_weights=bm_weights)
                            best_idea.insert_bulk(pe.id, inputs.holding_date, 'full_universe', best_idea.df_to_rows(universe_df, provider_etf_id=pe.id, value_date=inputs.holding_date, benchmark_mode='full_universe'))

                    generated_etfs += 1

                except Exception as e:
                    record_problem(batch_run_id=batch_run_id, provider=p, etf=pe, error=f"{e}", message=None, problem_etfs=problem_etfs)

            log.record_status(f"Completed processing provider {p.name}")

        batch_run.update_completed_at(batch_run_id)
        log.record_status(f"{log_prefix}Finished Best Ideas Generator batch run on {total_etfs} etfs.\n")
        return total_etfs, generated_etfs, problem_etfs

    except Exception as e:
        log.record_error(f"{log_prefix}Error in best ideas generator batch run: {e}")
        raise e
