import log
import os
from dataclasses import dataclass, field
from modules.core import sender
from modules.core.util import get_domain_from_url
from modules.object.provider import Provider, update_domain, getMappingFromJson
from modules.object.provider_etf import update_last_download
from modules.object.provider_etf_holding import insert_all_holdings
from modules.ticker.resolver import TickerResolver

from modules.parse.url import scrape_provider
from modules.parse.convert import load, map_data


@dataclass
class EtfStats:
    etf_name: str
    etf_id: int | None = None
    etf_region: str | None = None
    holdings: int = 0
    tickers: int = 0
    problem_tickers: list[str] = field(default_factory=list)
    error: str | None = None

    @property
    def problems(self) -> int:
        return self.holdings - self.tickers

    @property
    def match_pct(self) -> float:
        return 100.0 * self.tickers / self.holdings if self.holdings else 0.0


def process_provider(provider: Provider, save_dir: str | None = None) -> list[EtfStats]:
    if provider.id is None or provider.url_start is None:
        raise Exception("Missing data in DB for provider scraping.")

    try:
        if provider.domain is None:
            provider.domain = get_domain_from_url(provider.url_start)
            update_domain(provider)

    except Exception as e:
        sender.send_admin(subject=f"Failed to get Domain from URL", message=f"Failed to parse web age URL {provider.url_start} and get the domain.")
        raise Exception(f"Failed to get Domain from URL.")

    try:
        log.record_status(f"Starting to collect holdings from provider '{provider.name}'")

        downloads = scrape_provider(provider)

        if len(downloads) == 0:
            log.record_notice(f"No holdings downloads identified when scraping URL '{provider.name}'")
            return []

        log.record_status(f"Storing holdings for {len(downloads)} ETFs for analysis of provider '{provider.name}'.")

        stats: list[EtfStats] = []
        resolver = TickerResolver(TickerResolver.POPULATE_TICKER)

        for d in downloads:
            etf_stat = EtfStats(etf_name=d.etf.name or '', etf_id=d.etf.id, etf_region=d.etf.region)
            if d.etf and d.etf.id:
                try:
                    file_format = d.etf.file_format or d.provider.file_format
                    mapping = d.etf.mapping or d.provider.mapping
                    if mapping and d.file_name and d.data:
                        if save_dir:
                            file_name = f"{d.etf.id} - {d.etf.region} - {d.file_name}"
                            with open(os.path.join(save_dir, file_name), 'wb') as f:
                                f.write(d.data)

                        map_obj = getMappingFromJson(mapping)
                        full_rows = load(etf_name=d.etf.name, file_format=file_format, mapping=map_obj, file_name=d.file_name, raw_data=d.data)
                        df = map_data(full_rows=full_rows, file_name=d.file_name, date_from_page=d.date_from_page, mapping=map_obj)
                        etf_stat.holdings = len(df)

                        log.record_status(f"Resolving tickers for ETF '{d.etf.name}'...")
                        df['ticker_id'] = df.apply(
                            lambda row: resolver.resolve(
                                region=d.etf.region,
                                symbol=row.get('ticker'),
                                isin=row.get('isin'),
                                name=row.get('name'),
                            ),
                            axis=1
                        )
                        etf_stat.tickers = int(df['ticker_id'].notna().sum())
                        etf_stat.problem_tickers = sorted(set(
                            df.loc[df['ticker_id'].isna(), 'ticker'].dropna().astype(str).tolist()
                        ))
                        log.record_status(f"ETF '{d.etf.name}': {etf_stat.tickers}/{etf_stat.holdings} holdings identified as tickers ({etf_stat.match_pct:.1f}%).")

                        df = df[df['ticker_id'].notna()]
                        insert_all_holdings(d.etf.id, df)
                        update_last_download(d.etf.id)

                except Exception as e:
                    etf_stat.error = str(e)
                    log.record_error(f"Failed to parse the data for ETF '{d.etf.name}'. {e}")

            stats.append(etf_stat)

        log.record_status(f"Completed collection for the provider '{provider.name}'")
        return stats

    except Exception as e:
        message = f"The processing of the provider '{provider.name}' has not completed. {e}"
        log.record_error(message)
        sender.send_admin(subject=f"Failed holdings collection", message=f"{message}")
        raise Exception(message)
