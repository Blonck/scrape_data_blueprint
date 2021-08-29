import logging
import click
from pathlib import Path

from scraping.db import DbHandler
from scraping.scraping import get_teams_playoff, fetch_salary, get_team_base_stat_urls, get_player_stats_from_team_url


@click.command()
@click.argument('year', nargs=1, default=2021)
@click.option('--sqlite', type=click.Path(exists=False), default=None)
@click.option('--quiet', default=False, is_flag=True)
def fetch_and_print(year, sqlite, quiet):
    """
    Fetch salary and statistics of players from teams participated in the playoffs.

    By default it fetched data from the season 2020/2021 (year = 2021)

    On any error the scripts aborts.
    The script fetches all data first, and write the results into the DB afterwards.
    Thus if an error occurs while fetching the data, nothing is written to the DB.
    """
    if quiet:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(level=logging.INFO)

    if sqlite is not None:
        sqlite = Path(sqlite).resolve()
        sqlite = f'sqlite:///{sqlite}'
        logging.info(f'Using {sqlite} to store data')

    teams_playoff = get_teams_playoff(year)
    # extract only names of each team in playoff for later filtering
    team_names_playoff = {t.name for t in teams_playoff}

    # fetch all salaries
    salaries = fetch_salary(year)
    # filter for these where team was in playoff
    salaries = [p for p in salaries if p.team in team_names_playoff]

    team_base_urls = get_team_base_stat_urls()
    # filter for teams in playoff
    team_base_urls = {team: url for team, url in team_base_urls.items() if team in team_names_playoff}

    player_stats = []
    for team, url in team_base_urls.items():
        player_stats.extend(get_player_stats_from_team_url(team, url, year))

    nba_db = DbHandler(sqlite)
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
                                       salary_currecny=player.attributes['salary_currency'])
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


if __name__ == '__main__':
    fetch_and_print()
