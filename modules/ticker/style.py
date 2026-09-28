import log
from modules.object import ticker
from modules.object import categorize_ticker as cat_ticker_obj
from modules.calc import classification


def assign_styles() -> None:
    """
    Assigns a style to every valid company ticker that has none, in order of preference
    (ticker.type_from records the source): CAT_ETF — a constituent of a categorization ETF;
    PROVIDER_ETF — held by a provider ETF that describes itself as value or growth; MODEL — the
    classifier trained on the categorized constituents' factors. Runs after the master sync, so
    share-class siblings (master_ticker_id set) are skipped: they use their master's style.
    """
    ticker.update_style_for_unclassified()
    ticker.update_style_from_provider_etfs()
    log.record_status("Updated style/cap for newly created tickers.")

    training_data = cat_ticker_obj.fetch_all_for_style_classification()
    if training_data:
        items = [classification.to_categorize_ticker_item(t) for t in training_data]
        classifier = classification.get_classifier(items)
        classification.mark_style(classifier, ticker)
        log.record_status("Ran model classifier for NULL style_type tickers.")
