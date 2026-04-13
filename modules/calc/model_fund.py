import log
from datetime import date
from typing import List, Optional
from dataclasses import dataclass
from pydantic import BaseModel


# ── Shared strategy models ──────────────────────────────────────────────────

class Cap(BaseModel):
    name: str

class Style(BaseModel):
    name: str
    value: Optional[int] = None
    growth: Optional[int] = None

class Strategy(BaseModel):
    holdings: int
    cap: Cap
    style: Style
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

def to_fund_protocol(f) -> FundProtocol:
    """Convert any Fund dataclass (live or BT) to a FundProtocol."""
    return FundProtocol(id=f.id, name=f.name, strategy=f.strategy)


@dataclass
class FundHolding:
    fund_id: int
    symbol: str
    holding_date: date
    ranking: int
    source_etf_id: int
    max_delta: float | None


@dataclass
class FundHoldingChange:
    fund_id: int
    symbol: str
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
RANKING_GAP_DROP_FROM_ENTRY = 2
FETCH_RANKING_LEVEL = USE_RANKING_LOW + RANKING_GAP_DROP_FROM_ENTRY


# ── Helpers ──────────────────────────────────────────────────────────────────

def results_to_string(results: FundChangesResult, ticker_module) -> str:
    aggregator = ""
    all_symbols: set[str] = {ch.symbol for ch in results.changes}
    tickers = ticker_module.fetch_by_symbols(list(all_symbols))
    ticker_by_symbol = {t.symbol: t for t in tickers}

    aggregator += f"{results.fund.name}\n" + "=" * 20 + "\n"
    if not results.changes:
        aggregator += "No changes\n\n\n"
    else:
        aggregator += "{:<12}{:<15}{:<12}{:<15}{:<10}{}\n".format(
            "Direction", "Date", "Ranking", "Appearances", "Symbol", "Name"
        )
        for ch in results.changes:
            ticker = ticker_by_symbol.get(ch.symbol)
            date_str = ch.change_date.strftime("%Y-%m-%d") if ch.change_date else "---"
            aggregator += "{:<12}{:<15}{:<12}{:<15}{:<10}{}\n".format(
                ch.direction,
                date_str,
                ch.ranking if ch.ranking else "---",
                ch.appearances if ch.appearances else "---",
                ch.symbol,
                ticker.name if ticker else "---",
            )
        aggregator += "-" * 30 + "\n\n"

    return aggregator


# ── Core computation ─────────────────────────────────────────────────────────

def generate(
    today: date,
    fund: FundProtocol,
    previous_holdings: List[FundHolding],
    best_ideas_module,
) -> FundChangesResult:
    """
    Pure computation: determine today's holdings and changes for each fund.
    No DB access — callers are responsible for fetching previous holdings
    and saving the results.

    Parameters
    ----------
    today             : the date being processed
    fund              : the fund to process
    previous_holdings : prior holdings for this fund
    best_ideas_module : module exposing fetch_best_ideas_by_ranking(...)
    """
    results: FundChangesResult

    
    strategy = getStrategyFromJson(fund.strategy)
    provider_etfs = strategy.provider_etfs or []
    exchanges = strategy.exchanges or []

    if (
        strategy.style.name == "blend"
        and strategy.style.value is not None
        and strategy.style.growth is not None
    ):
        fetched_growth = best_ideas_module.fetch_best_ideas_by_ranking(
            ranking_level=FETCH_RANKING_LEVEL,
            style_type="growth",
            cap_type=strategy.cap.name,
            as_of_date=today,
            provider_etf_ids=provider_etfs,
            exchanges=exchanges,
            esg_only=strategy.esg_only,
        )
        fetched_value = best_ideas_module.fetch_best_ideas_by_ranking(
            ranking_level=FETCH_RANKING_LEVEL,
            style_type="value",
            cap_type=strategy.cap.name,
            as_of_date=today,
            provider_etf_ids=provider_etfs,
            exchanges=exchanges,
            esg_only=strategy.esg_only,
        )

        if USE_RANKING_HIGH != 1:
            fetched_growth = [i for i in fetched_growth if USE_RANKING_HIGH <= i.ranking]
            fetched_value = [i for i in fetched_value if USE_RANKING_HIGH <= i.ranking]

        fetched = fetched_growth + fetched_value

        growth_in_range = [i for i in fetched_growth if i.ranking <= USE_RANKING_LOW]
        value_in_range = [i for i in fetched_value if i.ranking <= USE_RANKING_LOW]

        growth_count = min(len(growth_in_range), round(strategy.holdings * strategy.style.growth / 100))
        value_count = min(len(value_in_range), strategy.holdings - growth_count)
        ideal_holdings = growth_in_range[:growth_count] + value_in_range[:value_count]
    else:
        fetched = best_ideas_module.fetch_best_ideas_by_ranking(
            ranking_level=FETCH_RANKING_LEVEL,
            style_type=strategy.style.name,
            cap_type=strategy.cap.name,
            as_of_date=today,
            provider_etf_ids=provider_etfs,
            exchanges=exchanges,
            esg_only=strategy.esg_only,
        )

        if USE_RANKING_HIGH != 1:
            fetched = [i for i in fetched if USE_RANKING_HIGH <= i.ranking]

        in_range = [i for i in fetched if i.ranking <= USE_RANKING_LOW]
        ideal_holdings = in_range[:strategy.holdings]

    holdings_changed: List[FundHoldingChange] = []
    todays_holdings: List[FundHolding] = []

    if not ideal_holdings:
        for ph in previous_holdings:
            ph.holding_date = today
            todays_holdings.append(ph)
        log.record_status(
            f"Ideal holdings empty for '{fund.name}' (prices not available relative to holding date). "
            f"Carried over {len(todays_holdings)} holdings."
        )
    else:
        for ph in previous_holdings:
            found_in_ideal = next((x for x in ideal_holdings if x.symbol == ph.symbol), None)
            if found_in_ideal is None:
                found_in_fetched = next((x for x in fetched if x.symbol == ph.symbol), None)
                if found_in_fetched is None:
                    holdings_changed.append(FundHoldingChange(
                        fund_id=fund.id, symbol=ph.symbol, change_date=today,
                        direction="sell", reason="Not in best ideas top levels",
                    ))
                    continue
                if found_in_fetched.ranking - ph.ranking >= RANKING_GAP_DROP_FROM_ENTRY:
                    holdings_changed.append(FundHoldingChange(
                        fund_id=fund.id, symbol=ph.symbol, change_date=today,
                        direction="sell", reason="Dropped below min ranking",
                    ))
                    continue

            ph.holding_date = today
            todays_holdings.append(ph)

        missing = strategy.holdings - len(todays_holdings)
        if missing > 0:
            existing_symbols = {th.symbol for th in todays_holdings}
            for fi in ideal_holdings:
                if fi.symbol in existing_symbols:
                    continue
                todays_holdings.append(FundHolding(
                    fund_id=fund.id, holding_date=today, symbol=fi.symbol,
                    ranking=fi.ranking, source_etf_id=fi.source_etf_id, max_delta=fi.max_delta,
                ))
                holdings_changed.append(FundHoldingChange(
                    fund_id=fund.id, symbol=fi.symbol, change_date=today, direction="buy",
                    ranking=fi.ranking, appearances=fi.appearances, max_delta=fi.max_delta,
                    top_delta_provider_etf_id=fi.source_etf_id, all_provider_etf_ids=fi.all_provider_ids,
                ))
                existing_symbols.add(fi.symbol)
                missing -= 1
                if missing == 0:
                    break

    return FundChangesResult(fund=fund, holdings=todays_holdings, changes=holdings_changed)
