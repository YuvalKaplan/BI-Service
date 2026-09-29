"""
Fills ticker.average_turnover (FMP profile averageVolume x price) now for every valid listing
that shares its company with another one on the same country's exchanges, in the same currency
(company.turnover_group_key — the only listings company.thin_lines compares), instead of
waiting up to a week for the profile refresh.
Run scripts/data_fill_master_tickers.py afterwards to move masters off the thin lines.
"""
import atexit
from collections import defaultdict
from modules.core import api_stocks
from modules.object import ticker
from modules.object.exit import cleanup
from modules.ticker import company
from modules.ticker import util as tu
from modules.ticker.resolver import TickerResolver

atexit.register(cleanup)


if __name__ == '__main__':
    valid = [t for t in ticker.fetch_all() if not t.invalid]
    by_id = {t.id: t for t in valid}
    lines: dict[tuple, list] = defaultdict(list)
    for t in valid:
        lines[(company.company_id(t, by_id), *company.turnover_group_key(t))].append(t)
    groups = [ts for ts in lines.values() if len(ts) > 1]
    todo = [t for ts in groups for t in ts]
    print(f"{len(todo)} listing(s) in {len(groups)} company/market group(s) with more than one line")

    resolver = TickerResolver(TickerResolver.POPULATE_TICKER)
    pairs: list[tuple[int, float]] = []
    for i, t in enumerate(todo, 1):
        profile = api_stocks.get_stock_profile(resolver.get_full_symbol(t))
        turnover = tu.profile_turnover(profile) if isinstance(profile, dict) else None
        if turnover is not None:
            pairs.append((t.id, turnover))
            t.average_turnover = turnover
        if i % 100 == 0:
            print(f"  {i}/{len(todo)}")
    ticker.update_average_turnover_bulk(pairs)
    print(f"Turnover stored for {len(pairs)} of {len(todo)} listing(s)")

    for ts in groups:
        thin = company.thin_lines(ts)
        if thin:
            main = max((t for t in ts if not company.is_secondary_line(t)), key=lambda t: t.average_turnover or 0)
            print(f"  {main.symbol}:{main.exchange} — thin: " + ', '.join(
                f"{t.symbol} ({t.average_turnover / main.average_turnover:.1%})" for t in ts if t.id in thin))
