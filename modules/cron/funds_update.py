import log
import pandas as pd
from datetime import date, timedelta
from typing import List
from modules.object import batch_run
from modules.object import best_idea, fund, fund_analysis, fund_holding, fund_holding_change, provider_etf, ticker
from modules.calc import model_fund
from modules.cron import best_ideas_generator
from modules.ticker import company

COMPANY_CAP_WINDOW_DAYS = 10  # matches best_idea.fetch_all_as_df's market-cap lookback


def build_shared_context(as_of_date: date) -> tuple[pd.DataFrame, dict]:
    """
    Builds the (all_best_ideas_df, mc_map) inputs shared by every fund for a given
    as_of_date. Computed once per run/date and passed into activate_fund() rather
    than recomputed per fund.
    """
    all_best_ideas_df = best_idea.fetch_all_as_df(as_of_date=as_of_date)
    all_best_ideas_df = model_fund.resolve_canonical_ticker_ids(all_best_ideas_df)

    # A multi-listing company's master is measured by the company's cap as of the date (its
    # first listing, in primary-listing order, with a value near the date — the master's own
    # listing can be a secondary one), not by the master listing's own cap. Feeds the large-cap
    # filter and the market-cap weighting (mc_map).
    if not all_best_ideas_df.empty:
        company_caps = company.company_caps_as_of(
            [int(t) for t in all_best_ideas_df['ticker_id'].unique()], as_of_date, COMPANY_CAP_WINDOW_DAYS)
        if company_caps:
            all_best_ideas_df['market_cap'] = [
                company_caps.get(int(t), mc) for t, mc in zip(all_best_ideas_df['ticker_id'], all_best_ideas_df['market_cap'])
            ]

    canonical_rows = all_best_ideas_df[
        all_best_ideas_df['ticker_id'] == all_best_ideas_df['canonical_ticker_id']
    ]
    mc_map = (
        canonical_rows[['canonical_ticker_id', 'market_cap']]
        .dropna(subset=['market_cap'])
        .drop_duplicates(subset='canonical_ticker_id')
        .set_index('canonical_ticker_id')['market_cap']
        .to_dict()
    )

    return all_best_ideas_df, mc_map


def activate_fund(
    fund_id: int,
    as_of_date: date,
    all_best_ideas_df: pd.DataFrame,
    mc_map: dict,
) -> model_fund.FundChangesResult | None:
    """
    Updates a single fund's holdings for as_of_date and persists the result.
    Returns None if the fund's strategy.recalc_frequency_days hasn't elapsed
    since its last recalculation (the fund's holdings are left untouched).
    """
    f = fund.fetch_by_id(fund_id)
    if f is None:
        raise Exception(f"Fund not found: fund_id={fund_id}")

    fund_protocol = model_fund.to_fund_protocol(f)
    previous_eval_date = as_of_date - timedelta(days=1)
    strategy = model_fund.getStrategyFromJson(fund_protocol.strategy)

    previous_holdings = fund_holding.fetch_funds_holdings(fund_id, previous_eval_date)
    _to_current_masters(previous_holdings)

    if previous_holdings:
        days_since_recalc = (as_of_date - previous_holdings[0].holding_date).days
        if days_since_recalc < strategy.recalc_frequency_days:
            log.record_status(
                f"[funds_update] Skipping '{f.name}' — {days_since_recalc}d since last recalculation, "
                f"frequency is {strategy.recalc_frequency_days}d."
            )
            return None

    fund_ideas_df = all_best_ideas_df[
        all_best_ideas_df['benchmark_mode'] == strategy.benchmark
    ]

    results = model_fund.generate(
        today=as_of_date,
        fund=fund_protocol,
        previous_holdings=previous_holdings,
        all_best_ideas_df=fund_ideas_df,
        mc_map=mc_map,
    )

    fund_holding.insert_fund_holding(results.holdings)
    fund_holding_change.insert_fund_changes(results.changes)
    _record_fund_analysis(fund_id, as_of_date, strategy, fund_ideas_df)

    log.record_status(model_fund.results_to_string(results))
    return results


def _to_current_masters(holdings: list) -> None:
    """A holding is stored under its company's master id at the time; one whose master has
    since moved to a better primary listing (master.align_masters_to_primary) is carried over
    under the company's current master — the id the best ideas now use — so the same company
    isn't sold and bought back."""
    info = ticker.fetch_master_info_by_ids([h.ticker_id for h in holdings])
    for h in holdings:
        master_id = info.get(h.ticker_id, (None, None))[0]
        if master_id:
            h.ticker_id = master_id


def _record_fund_analysis(
    fund_id: int,
    as_of_date: date,
    strategy: model_fund.Strategy,
    fund_ideas_df: pd.DataFrame,
) -> None:
    """
    Snapshots, into fund_analysis, the full active-weight calculation of every ETF this fund
    drew its best ideas from on as_of_date — every company, not only the top-ranked ones — with
    the benchmark actually applied. Recomputed with the same best_ideas_generator helpers that
    produced best_idea, from the same holdings date, and cross-checked against it. Failures are
    logged and never block the fund update itself.
    """
    try:
        bm_cache: dict = {}
        rows: list[fund_analysis.FundAnalysis] = []
        for etf_id, value_date in model_fund.etfs_used(strategy, fund_ideas_df).items():
            pe = provider_etf.fetch_by_id(etf_id)
            inputs = best_ideas_generator.prepare_etf_inputs(pe, value_date)
            if inputs is None or inputs.holding_date != value_date:
                log.record_notice(f"[fund_analysis] fund_id={fund_id}: no holdings for ETF {etf_id} on {value_date} — skipped.")
                continue

            benchmark_id = benchmark_date = None
            bm_weights = None
            if strategy.benchmark == 'full_universe':
                if not pe.benchmark_id:
                    continue
                bm_weights, benchmark_date = best_ideas_generator.get_benchmark_weights(pe.benchmark_id, as_of_date, bm_cache)
                if not bm_weights:
                    continue
                benchmark_id = pe.benchmark_id

            weights = best_ideas_generator.compute_active_weights(inputs, bm_weights)
            selected = best_ideas_generator.select_best_ideas(weights, best_ideas_generator.MAX_BEST_IDEAS_PER_FUND)

            stored = best_idea.fetch_for_etf_date(etf_id, value_date, strategy.benchmark)
            if [s.ticker_id for s in stored] != [int(t) for t in selected['ticker_id']] or any(
                abs((s.delta or 0) - d) > 1e-9 for s, d in zip(stored, selected['delta'])
            ):
                log.record_notice(
                    f"[fund_analysis] fund_id={fund_id}: recomputed best ideas for ETF {etf_id} on {value_date} "
                    f"differ from stored best_idea ({strategy.benchmark}) — stored snapshot reflects the recomputation."
                )

            rows += best_ideas_generator.build_analysis_rows(fund_id, as_of_date, inputs, weights, selected, benchmark_id, benchmark_date)

        fund_analysis.replace_for_fund_date(fund_id, as_of_date, rows)
    except Exception as e:
        log.record_error(f"[fund_analysis] Failed to record analysis for fund_id={fund_id} on {as_of_date}: {e}")


def run() -> List[model_fund.FundChangesResult]:
    try:
        batch_run_id = batch_run.insert(batch_run.BatchRun(process="funds_update", activation="auto"))

        funds = fund.fetch_all()
        log.record_status(f"Running Fund Update batch job ID {batch_run_id} - will process {len(funds)} funds.")

        today = date.today()
        all_best_ideas_df, mc_map = build_shared_context(today)

        all_results = [
            r for r in (activate_fund(f.id, today, all_best_ideas_df, mc_map) for f in funds)
            if r is not None
        ]

        batch_run.update_completed_at(batch_run_id)
        log.record_status("Finished Fund Update batch run.\n")
        return all_results

    except Exception as e:
        log.record_error(f"Error in fund update batch run: {e}")
        raise e
