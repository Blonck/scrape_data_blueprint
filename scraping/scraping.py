import re
import logging
from urllib.request import urlopen
from urllib.error import HTTPError
from urllib.error import URLError
from typing import List

from bs4 import BeautifulSoup

from .models import TeamModel, PlayerYearModel, PlayerYearSeasonModel

logger = logging.getLogger(__name__)


class UnexpectedPageContent(Exception):
    """
    Exception raised when page content does not match expectation

    The structure or the content of the page may have changed
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


def get_bs(url):
    """
    Load `url` and put it into a BeautifulSoup object

    Logs exceptions and reraise them
    """
    try:
        bs = BeautifulSoup(urlopen(url).read(), 'html.parser')
    except HTTPError as e:
        logger.error(f'{e}')
        raise e
    except URLError as e:
        logger.error(f'URL not found: {url}')
        raise e
    else:
        return bs


def fetch_teams_playoff(year) -> List[TeamModel]:
    """Return teams participated in playoffs in given year"""

    # use some statistic site which shows postseason stats of a given year to retrieve list of teams participated
    # in playoffs kind of statistics doesn't matter
    url_teams = f'https://www.espn.com/nba/stats/team/_/season/{year}/seasontype/3/table/offensive/sort/avgPoints/dir/desc'

    bs = get_bs(url_teams)
    # teams have links with `data-clubhouse-uid` attribute
    team_links = bs.find_all('a', {'data-clubhouse-uid': True})

    if len(team_links) != 16:
        raise UnexpectedPageContent("Number of teams in playoffs must be 16")

    return [TeamModel(name=a.get_text(), attributes={'data-clubhouse-uid': a['data-clubhouse-uid']})
            for a in team_links]


def fetch_salaries_from_url(url) -> List[PlayerYearModel]:
    """Extract salary of all players from the given url"""
    bs = get_bs(url)
    # first find the salary table
    salary_table = bs.find_all('table', {'class': 'tablehead'})

    # from which one should exits
    if len(salary_table) != 1:
        raise UnexpectedPageContent("Found multiple/no salary table")
    salary_table = salary_table[0]

    # every row in this table contains name and salaray of a player
    player_salaries = []
    for row in salary_table.find_all('tr', {'class': re.compile('(oddrow)|(evenrow)')}):
        elems = row.find_all('td')
        # second element is the name of the player + postion
        name = elems[1].get_text()
        name, position = name.split(',')[0], name.split(',')[1]

        # third element is the team name of the player
        team = elems[2].get_text()
        # fourth element is the salary in the form `$10,000,000`
        # remove the leading '$' from salary and drop the `,`
        salary = elems[3].get_text()

        if salary[0] != '$':
            raise UnexpectedPageContent("Concurenccy symbol is not $")

        try:
            salary = int(salary[1:].replace(',', ''))
        except ValueError:
            raise UnexpectedPageContent("Salary converion to integer failed")

        att = {'salary': salary, 'salary_currency': '$', position: position}
        player = PlayerYearModel(name=name,
                                 team=team,
                                 year=2021,
                                 attributes=att)
        player_salaries.append(player)

    return player_salaries


def player_salary_url(year, page):
    """
    Construct URL for a given year on espn.com

    Args:
        year: Year of the salray list
        page: Number of the page of the salary list. Larger page numbers lead to valid URLs but with
        empy tables.
    """

    # seasontype seams to make no difference,
    # salaries are paginated,
    return f'https://www.espn.com/nba/salaries/_/year/{year}/page/{page}/seasontype/4'


def fetch_salary(year):
    """
    Fetch salary of all players for a given year

    The salary pages are paginated, so we go trough all until
    we retrive a salary page with an empty list. (All page
    numbers exists, no 404)
    """
    page = 1
    salaries_full = []
    while True:
        logger.debug(f'Retrieving salaries for {year}, page {page}')
        url_player_salaries = player_salary_url(year, page)
        salaries = fetch_salaries_from_url(url_player_salaries)

        if len(salaries) == 0:
            logger.debug(f'Fetched all salaries for {year}')
            break
        elif page > 50:
            raise UnexpectedPageContent("Aborted fetching salaries, too many sited")
        else:
            salaries_full.extend(salaries)
            page += 1

    return salaries_full


def fetch_team_base_stat_urls():
    """Fetch list of all urls for team statistics"""
    urls = {}

    team_url = 'https://www.espn.com/nba/teams'
    bs = get_bs(team_url)

    # first find section with tables of teams
    sections = bs.find_all('section', {'class': 'TeamLinks flex items-center'})

    if len(sections) != 30:
        raise UnexpectedPageContent("More than 30 teams fetched on team statistics page")

    for section in sections:
        headers = section.find_all('h2')
        if len(headers) != 1:
            raise UnexpectedPageContent("Unexpected number of h2 headers")

        team = headers[0].get_text()

        stat_url = section.find_all('a', {'href': re.compile('\/nba\/team\/stats.*')})
        if len(stat_url) != 1:
            raise UnexpectedPageContent("Found multiple/none number of stats links")

        stat_url = stat_url[0]
        if stat_url.get_text() != 'Statistics':
            UnexpectedPageContent("No Statistics text in URL")

        stat_url = stat_url.attrs['href']
        # remove last subpath from URL
        stat_url = stat_url[:stat_url.rindex('/')]

        urls[team] = f'https://www.espn.com{stat_url}'
    return urls


def fetch_player_stats_from_team_url(team, team_stat_base_url, year):
    """Retrieve player statistics from team url for given year (postseason)"""
    team_stat_url = f'{team_stat_base_url}/season/{year}/seasontype/3'

    # for now fetch only postseason stats
    bs = get_bs(team_stat_url)

    # find div with `Player Stats`
    stat_tables = bs.find_all('div', {'class': re.compile("(ResponsiveTable).*")})

    stat_table = []
    for table in stat_tables:
        if 'Player Stats' in table.find_all('div', {'class': 'Table__Title'})[0].get_text():
            stat_table.append(table)

    if len(stat_table) != 1:
        raise UnexpectedPageContent(f"Number of Player Stats tables does not fit: {len(stat_table)}")
    stat_table = stat_table[0]

    # extract players from table
    player_links = stat_table.find_all('a', {'class': "AnchorLink", 'data-player-uid': True})
    players = [a.get_text() for a in player_links]
    # should be 14 according to Wikipedia, but found lower numbers
    if len(players) < 8:
        raise UnexpectedPageContent(f"Unreasonable number of players {len(players)} for team {team} in {year}")

    data_table = stat_table.find_all('table', {'class': 'Table Table--align-right'})

    if len(data_table) != 1:
        raise UnexpectedPageContent(f"Number of data tables does not fit: {len(data_table)}")
    data_table = data_table[0]

    data_rows = data_table.find_all('tr', {'class': "Table__TR Table__TR--sm Table__even", 'data-idx': True})

    # last row is just team total
    if len(data_rows) != len(players) + 1:
        raise UnexpectedPageContent("Number of rows for player and data table does not fit")
    data_rows = data_rows[:-1]

    col_names = ['games_played', 'games_started', 'minutes_per_game', 'points_per_game', 'offensive_rebounds_per_game',
                 'defensive_rebounds_per_game', 'rebounds_per_game', 'assists_per_game', 'steals_per_game',
                 'blocks_per_game', 'turnovers_per_game', 'fouls_per_game',
                 'assists_to_turnover_ratio', 'player_efficency_rating']
    # TODO check that header of table fits to assumed columns above

    result = []

    for player, row in zip(players, data_rows):

        cols = row.find_all('span', {'class': ""})
        atts = {}

        if len(col_names) != len(cols):
            logger.error('Number of statistics does not fit')
            logger.error(col_names)
            logger.error([c.get_text() for c in cols])

        for col_name, col in zip(col_names, cols):
            atts[col_name] = col.get_text()

        p = PlayerYearSeasonModel(name=player, team=team, year=year, season='postseason', attributes=atts)
        result.append(p)

    return result
