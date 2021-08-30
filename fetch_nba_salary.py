import logging
import click
from pathlib import Path

from scraping.db import DbHandler
from scraping.scraping import fetch_teams_playoff, fetch_salary, fetch_team_base_stat_urls, fetch_player_stats_from_team_url


def set_logging_level(quiet, debug):
    """Set the logging level based on `quiet` and `debug` flag"""
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        if quiet:
            logging.basicConfig(level=logging.ERROR)
        else:
            logging.basicConfig(level=logging.INFO)


def print_salaries_as_csv(salaries):
    """Print salaries of players to console"""
    print('#Player,Team,Salary')
    for player in salaries:
        salary = f'{player.attributes["salary_currency"]}{player.attributes["salary"]}'
        print(f'{player.name},{player.team},{salary}')


@click.command()
@click.argument('year', nargs=1, default=2021)
@click.option('--sqlite', type=click.Path(exists=False), default=None)
@click.option('--quiet', default=False, is_flag=True)
@click.option('--debug', default=False, is_flag=True)
@click.option('--skip-scraping', default=False, is_flag=True)
def fetch_and_print(year, sqlite, quiet, debug, skip_scraping):
    """
    Fetch salary and statistics of players from teams participated in the playoffs.

    By default it fetched data from the season 2020/2021 (year = 2021)

    On any error the scripts aborts.
    The script fetches all data first, and write the results into the DB afterwards.
    Thus if an error occurs while fetching the data, nothing is written to the DB.
    """
    set_logging_level(quiet, debug)

    if sqlite is not None:
        sqlite = Path(sqlite).resolve()
        sqlite = f'sqlite:///{sqlite}'
        logging.debug(f'Using {sqlite} to store data')

    nba_db = DbHandler(sqlite)
    if not skip_scraping:
        logging.info('Scraping teams...')
        teams_playoff = fetch_teams_playoff(year)
        # extract only names of each team in playoff for later filtering
        team_names_playoff = {t.name for t in teams_playoff}

        # fetch all salaries
        logging.info('Scraping salaries...')
        salaries = fetch_salary(year)
        # filter for these where team was in playoff
        salaries = [p for p in salaries if p.team in team_names_playoff]

        team_base_urls = fetch_team_base_stat_urls()
        # filter for teams in playoff
        team_base_urls = {team: url for team, url in team_base_urls.items() if team in team_names_playoff}

        logging.info('Scraping player statistics...')
        player_stats = []
        for team, url in team_base_urls.items():
            player_stats.extend(fetch_player_stats_from_team_url(team, url, year))

        # store teams in DB
        logging.info('Insert teams into DB...')
        for team in team_names_playoff:
            logging.debug(f'Insert team `{team}` into DB')
            nba_db.merge_team(team)
            nba_db.merge_playoff_team(team, year)

        logging.info('Insert player (salaries) into DB...')
        for player in salaries:
            logging.debug(f'Insert player `{player.name}` into DB')
            nba_db.merge_player(player.name)
            nba_db.merge_team_player(team=player.team, player=player.name, year=year)
            if 'salary' in player.attributes and 'salary_currency' in player.attributes:
                nba_db.merge_player_salary(player=player.name, year=year,
                                           salary=player.attributes['salary'],
                                           salary_currency=player.attributes['salary_currency'])
            else:
                logging.error(f'Could not find salary in player `{player.name}` attributes: `{player.attributes}`')

        logging.info('Insert player statistics into DB...')
        for player in player_stats:
            logging.debug(f'Insert player `{player.name}` statistics into DB')
            # player and team_player table should be already filled,
            # but as we take the salary and the stats from two different pages,
            # they may disagree, so store here again every player
            nba_db.merge_player(player.name)
            nba_db.merge_team_player(team=player.team, player=player.name, year=year)

            stats = {}
            try:
                stats['games_played'] = int(player.attributes['games_played'])
                stats['points_per_game'] = float(player.attributes['points_per_game'])
                stats['assists_per_game'] = float(player.attributes['assists_per_game'])
                stats['rebounds_per_game'] = float(player.attributes['rebounds_per_game'])
                stats['minutes_per_game'] = float(player.attributes['minutes_per_game'])
            except KeyError as e:
                logging.error(f'Missing statistics for {player} in attributes: {e}')
                logging.error(player.attributes)
            except ValueError as e:
                logging.error(f'Cannot convert statistic for {player} to type: {e}')
                logging.error(player.attributes)
            else:
                nba_db.merge_player_stats(player=player.name, year=year, stats=stats)

    # print salaries to csv
    top_salaries = nba_db.fetch_player_salary_playoffs(year, limit=10)
    print_salaries_as_csv(top_salaries)


if __name__ == '__main__':
    fetch_and_print()
