# Insightful Investments - Services
Services to accept emails from users and analyse them.

## Python

### Vertual Environment
Install a vertual environment at the command prompt with:
`python -m venv .venv`
This can then be ctivated with:
`.venv/Scripts/activate`

Currently using modules:
dotenv psycopg psycopg-pool psycopg_binary flask tld waitress bcrypt httpx playwright playwright-stealth beautifulsoup4 pypdf pybars3 num2words pycountry python-dateutil mailgun pydantic openai

### Package manager
The packages are installed using the `pip` command, for example:
`pip install Flask`

Updating the requirements.txt file is done by running the following in the command line:
`pip freeze > requirements.txt`

To find out about dependencies use `pip show library-name`

#### Reset the Virtual Environment:
- At the terminal prompt: `deactivate`
- Delete the `.venv` directory
- Run at the terminal: `python -m venv .venv`
- Re-install all the above dependancies: `pip install ...list above...`
- Generate a new requirements.txt

## Web Server
We are using default [Flask](hhttps://flask.palletsprojects.com/en/stable/) during development (service_dev.py) and [Waitress](https://flask.palletsprojects.com/en/stable/deploying/waitress/) (service.py) in production.

### Webhooks
To test webhooks from MailGun on a development (localhost) we use [ngrok](https://dashboard.ngrok.com/get-started/setup/windows) to forward the POST request.

After it is installed, run it in a terminal and run the command: `ngrok http --url=decent-cattle-certain.ngrok-free.app 10000`
The display in the terminal shows session status and the `Forwarding` URL. 

We use a [free static domain](https://dashboard.ngrok.com/domains) with URL: `https://decent-cattle-certain.ngrok-free.app`.

The production URL is: `https://services.insightful.investments`

Activity can be monitored [here](https://dashboard.ngrok.com/traffic-inspector) (login currently under yuvalkn1 Gmail credentials).

## Database

As we use the DB as the Single Source of Truth, we simply use [psycopg](https://www.psycopg.org/) library for connection pool management and CRUD actions.
We use the [psycopg.rows](https://www.psycopg.org/psycopg3/docs/advanced/rows.html) utility to generate the data as classes.
Classes are created with the [dataclass](https://www.datacamp.com/tutorial/python-data-classes) wrapper.


### Vector Database for LLM
We are using the [pgvector](https://github.com/pgvector/pgvector) extension of Postgres SQL that enables storing and then searching based on vector transformations.
In order to install on a Windows 11 desktop, you will need to run the `nmake` command in a `x64 Native Tools Command Prompt for Visual Studio` window. For this to work you need to install the Visual Studio Build Tools for the Windows OS version. In the "Installation details" pane on the right side of the installer, expand the "Desktop development with C++" workload.
Make sure the "Windows 11 SDK" (or the latest version available) is checked. Restart the x64 Native Tools Command Prompt - *Run as administrator*. 
Run:
> set "PGROOT=C:\Program Files\PostgreSQL\17"
> cd %TEMP%\pgvector
> nmake /F Makefile.win
> nmake /F Makefile.win install

Then in the DB prompt as the user `postgres` (with superuser access), run:
> CREATE EXTENSION vector;

Render.com supports the vector extension. To get is activated do the following (works also as 'admin' user):
> CREATE EXTENSION vector;

## AI
We use [Google AI SDK](https://ai.google.dev/gemini-api/docs) for analysis of the PDF and HTML files (via links in the email).

## Playwright
We are using this library to simulate activity in a web browser. We are using the [headless version](https://playwright.dev/python/docs/browsers).

### In order to give the maximum possabilities to scrape as many pages as possible we have a few methods that can be used:
1. wait_on_selector: This is a selector in the DOM that Playwright will wait for to be visible. This is versifies after the page is loaded.
2. events: These are a series of recorded steps that need to run (usualy user identification and cookie acceptance). After these run the page is usually loaded with the desired content.
3. content_selector: This has two functions, firstly, after the events are run, this verifies that the selector is visible. Secondly, using this selector we reduce the amount of HTML hthat is considered for extraction of URL's to documents.
4. levels: This enables the scraper to dig into a secondary or more level of pages. In the majority of cases this is set to 1 as most content is located at the first level.

#### What are "selectors"
A selector is a combination of the tag tipe and an attribute value, for example:
- 'div.content' = A div tag with a class "content"
- 'section#list-of-items' = A section tag with an id "list-of-items"

#### Event recording
We use that [Playwright CRX Chrome browser plugin](https://chromewebstore.google.com/detail/jambeljnbnfbkcpnoiaedcabbgmnnlcd) to record the events. We then copy the JSONL (JSON Lines) to the Database events column and make it an array. These are then played back on request in the dispacher function of the url scraper.
We have added non recordable events that can be added after recording:
- mouse: scroll in x/y
- scroll_to_first: scroll to the first instance of a selector.
