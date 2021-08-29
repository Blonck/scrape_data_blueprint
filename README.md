# Blueprint scraping NBA data via BeatifulSoup

A personal blueprint of how to use BeatifulSoup to scrap some data,
executed to scrap salaries of NBA players.

## Environment

This repository uses python virtual environments to handle the package management.

### Setup python venv

To setup the working environment, create a python virtual environment
within the project directory.

```bash
python3 -m venv .venv
```

### Activate venv

```bash
source .venv/bin/activate
```

### Install requirements

```bash
pip install -r requirements.txt
```

## How to execute

The following command will scrape the player salaries and statistics for the given year,
store it into an sqlite database in the local directory, and print the players with the top 10 salaries.
```bash
python fetch_nba_salary.py 2021 --quiet --sqlite ./nba.db
```
