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
dotenv psycopg psycopg-pool psycopg_binary tld bcrypt playwright playwright-stealth mailgun pydantic pandas openpyxl xlrd

#### Package manager
The packages are installed using the `pip` command, for example:
`pip install dotenv`

Updating the requirements.txt file is done by running the following in the command line:
`pip freeze > requirements.txt`

To find out about dependencies use `pip show library-name`

## Database

As we use the DB as the Single Source of Truth, we simply use [psycopg](https://www.psycopg.org/) library for connection pool management and CRUD actions.
We use the [psycopg.rows](https://www.psycopg.org/psycopg3/docs/advanced/rows.html) utility to generate the data as classes.
Classes are created with the [dataclass](https://www.datacamp.com/tutorial/python-data-classes) wrapper.

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
