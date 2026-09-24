import atexit
from modules.object.exit import cleanup
from modules.cron import categorize_downloader
from modules.object import ticker
from modules.object import categorize_ticker as cat_ticker_obj
from modules.calc import classification

atexit.register(cleanup)

if __name__ == "__main__":
    categorize_downloader.run()

     ## -- Classification ---
    ticker.update_style_for_unclassified()
    ticker.update_style_from_provider_etfs()
    print("Updated style/cap for newly created tickers.")

    training_data = cat_ticker_obj.fetch_all_for_style_classification()
    if training_data:
        items = [classification.to_categorize_ticker_item(t) for t in training_data]
        classifier = classification.get_classifier(items)
        classification.mark_style(classifier, ticker)
        print("Ran model classifier for NULL style_type tickers.")
