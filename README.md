# Best Ideas - Services
Services to scrape active ETF holdings and analyse the data.

## Python

### Vertual Environment
Install a vertual environment at the command prompt with:
`python -m venv .venv`
This can then be activated with:
`.venv/Scripts/activate`

#### Reset the Virtual Environment:
- At the terminal prompt: `deactivate`
- Delete the `.venv` directory
- Run at the terminal: `python -m venv .venv`
- Re-install all the dependancies: `pip install ...list above...`
- Generate a new requirements.txt

#### Tell VS Code to use the venv’s Python interpreter
- Press `Ctrl+Shift+P`
- Type Python: Select Interpreter
- Choose: `.venv\Scripts\python.exe` (Windows)

### Modules
#### Currently using modules:
`dotenv psycopg psycopg-pool psycopg_binary tld bcrypt playwright playwright-stealth mailgun pydantic pandas openpyxl xlrd`

##### Only needed by classification (used in back testing only):
`scikit-learn`

##### Factset required modules:
Used only once for back testing data download.
`fds.sdk.utils fds.sdk.FactSetOwnership fds.sdk.Formula`

#### Package manager
The packages are installed using the `pip` command, for example:
`pip install dotenv`

Updating the requirements.txt file is done by running the following in the command line:
`pip freeze > requirements.txt`

To find out about dependencies use `pip show library-name`

## Database

### Single Source of Truth

As we use the DB as the Single Source of Truth, we simply use [psycopg](https://www.psycopg.org/) library for connection pool management and CRUD actions.
We use the [psycopg.rows](https://www.psycopg.org/psycopg3/docs/advanced/rows.html) utility to generate the data as classes.
Classes are created with the [dataclass](https://www.datacamp.com/tutorial/python-data-classes) wrapper.

## Transfer Production database to Development
All of the following should be done in pgAdmin:
1. Create a data only backup of Production database in a CMD window:
    > pg_dump  -h  dpg-d5do2kje5dus739gfud0-a.virginia-postgres.render.com -U admin -d best_ideas_eq6y --column-inserts --disable-triggers --data-only -f C:\Users\Yuval\Downloads\db_backup_data_only.sql
    - You will need the admin password - get this from render.com
2. Truncate all the tables in the development database in pgAdmin:
    > call truncate_all_tables();
3. Back in the CMD window, retore the database:
    > psql -h localhost -p 5432 -U admin -d best_ideas -f C:\Users\Yuval\Downloads\db_backup_data_only.sql
    - You will need the admin password

### Multi-Ticker (Share-Class) Consolidation

A company can trade as more than one independently-listed ticker (e.g. Alphabet as `GOOGL`/`GOOG`). `ticker.master_ticker_id` groups these (`NULL` on the master row, pointing at the master's `id` on every sibling), and `ticker.accumulated_market_cap` holds the combined market cap, populated only on the master row.

#### One-time schema migrations
Run each one-shot migration file against dev, then prod, in psql/pgAdmin, in order:
```
modules/object/_migration_7_ticker_master_ticker.sql
modules/object/_migration_8_ticker_updated_at.sql
```
Delete each file once it's been applied to both environments, per the usual convention for files under `modules/object/_migration_*.sql`.

#### Populating master tickers for the first time (live)
After the migrations have been applied to an environment, run these against it in order (pass `--dev` or `--prod`):
```
python scripts/data_fill_ticker_profile.py --dev       # refreshes cik (and full profile) for every stale ticker
python scripts/data_fill_master_tickers.py --dev       # elect masters + compute accumulated_market_cap
python scripts/current_benchmark_generator.py --dev    # today's live benchmark snapshot, now consolidated
```
`current_benchmark_generator.py` only *reads* `master_ticker_id` — it doesn't create it, so `data_fill_master_tickers.py` must be run at least once first or nothing will be consolidated.

`data_fill_ticker_profile.py` asks at a prompt whether to also retry tickers already marked invalid; answering no (the default) only checks tickers that are either brand new or haven't been checked in over a week (`ticker.updated_at`).

Ongoing maintenance runs automatically: ticker profile refresh on the Tue–Sat cron step, master sync + accumulated cap refresh on Wednesday before benchmark/best-ideas generation (see `service_cron.py`).

#### Preparing data for a historical simulation (sim)
`scripts/sim_prep_data.py` combines all three prerequisite steps above plus the historical benchmark backfill into one call (edit `inception_date` in the file first; it asks the same retry-invalid prompt as `data_fill_ticker_profile.py`, since it goes over the exact same ticker list):
```
python scripts/sim_prep_data.py --dev
```
Run this before `sim_fund.py`. The ticker profile refresh and master sync steps are idempotent, so it's safe to re-run even if the live steps above already populated them.

## Playwright
We are using this library to simulate activity in a web browser. We are using the [headless version](https://playwright.dev/python/docs/browsers).

Development: we can switch to `headless=False` to view how the browser is performing the events and the download trigger.

### In order to give the maximum possabilities to scrape as many pages as possible we have a few methods that can be used:
1. wait_pre/post_events: This is a selector in the DOM that Playwright will wait for to be visible.
2. events: These are a series of recorded steps that need to run (usualy user identification and cookie acceptance). After these run the page is usually loaded with the desired content.

These are available at the domain level and at each ETF level if need be.

#### What are "selectors"
A selector is a combination of the tag tipe and an attribute value, for example:
- 'div.content' = A div tag with a class "content"
- 'section#list-of-items' = A section tag with an id "list-of-items"

### Event recording
We use that [Playwright CRX Chrome browser plugin](https://chromewebstore.google.com/detail/jambeljnbnfbkcpnoiaedcabbgmnnlcd) to record the events. We then copy the JSONL (JSON Lines) to the Database events column and make it an array. These are then played back on request in the dispacher function of the url scraper.
We have added non recordable events that can be added after recording:
- mouse: scroll in x/y
- scroll_to_first: scroll to the first instance of a selector.
