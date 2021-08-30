import pandas as pd
import numpy as np

from typing import List, Optional, Dict, Union

from sqlalchemy import MetaData, create_engine
from sqlalchemy import Table, Column, UniqueConstraint, CheckConstraint, ForeignKey, String, Integer
from sqlalchemy import select, and_, insert, update

from .models import PlayerYearModel


metadata_obj = MetaData()

teams = Table(
    'nba_teams', metadata_obj,
    Column('name', String(250), primary_key=True),
    comment="Names of all scraped NBA teams"
)

players = Table(
    'nba_players', metadata_obj,
    Column('name', String(250), primary_key=True),
    comment="Names of all scraped NBA players"
)

playoff_team = Table(
    'nba_playoff_team', metadata_obj,
    Column('id', Integer, primary_key=True),
    Column('year', Integer, nullable=False),
    Column('team_name', String(250), ForeignKey("nba_teams.name"), nullable=False),
    UniqueConstraint('year', 'team_name', name='uc_year_tname'),
    comment="Stores which teams participated in the playoffs for every year"
)

team_player = Table(
    'nba_team_player', metadata_obj,
    Column('id', Integer, primary_key=True),
    Column('player_name', String(250), ForeignKey("nba_teams.name"), nullable=False),
    Column('team_name', String(250), ForeignKey("nba_players.name"), nullable=False),
    Column('year', Integer, nullable=False),
    UniqueConstraint('year', 'player_name', name='uc_year_pname'),
    comment="Relation of team <-> player for each year, assumes that each player has only one team per year"
)

player_salaries = Table(
    'nba_player_salaries', metadata_obj,
    Column('id', Integer, primary_key=True),
    Column('player_name', String(250), ForeignKey("nba_players.name"), nullable=False),
    Column('year', Integer, nullable=False),
    Column('salary', Integer, nullable=False),
    Column('salary_currency', String(50), nullable=False),
    UniqueConstraint('year', 'player_name', name='uc_year_pname'),
    comment="Salary for each player per year"
)

player_stats = Table(
    'nba_player_stats', metadata_obj,
    Column('id', Integer, primary_key=True),
    Column('player_name', String(250), ForeignKey("nba_players.name"), nullable=False),
    Column('year', Integer, nullable=False),
    Column('season', String(250), nullable=False),
    Column('stat_name', String(250), nullable=False),
    Column('stat_value', String(16), nullable=False),
    Column('stat_type', String(16), nullable=False),
    UniqueConstraint('year', 'player_name', 'season', 'stat_name', name='uc_year_season_pname_stat'),
    CheckConstraint("season in ('postseason', 'regularseason')")
)


class DbHandler():
    """
    Access to NBA DB

    Will connect to an SQLite DB with the given file, if no file provided in-memory DB is used

    TODOS: provide interface which works with list of players/teams (optimize for executemany)
    """
    def __init__(self, sqlite_url=None):
        if not sqlite_url:
            sqlite_url = 'sqlite:///:memory:'

        self.engine = create_engine(sqlite_url)
        metadata_obj.create_all(self.engine)

    def merge_team(self, team: str) -> None:
        """Insert team into `nba_teams` if not existent"""
        with self.engine.connect() as c:
            stmt = select(teams).where(teams.c.name == team)
            r = c.execute(stmt).scalar()

            if r is None:
                stmt = insert(teams).values(name=team)
                c.execute(stmt)

    def merge_playoff_team(self, team: str, year: int) -> None:
        """Insert team into `nba_playoff_team` if not existent"""
        with self.engine.connect() as c:
            stmt = (
                select(playoff_team).
                where(and_(playoff_team.c.team_name == team, playoff_team.c.year == year))
            )
            r = c.execute(stmt).scalar()

            # insert team if not already in DB
            if r is None:
                stmt = (
                    insert(playoff_team).
                    values(team_name=team, year=year)
                )
                c.execute(stmt)

    def merge_player(self, player: str) -> None:
        """Insert player into `nba_players` if not existent"""
        with self.engine.connect() as c:
            stmt = select(players).where(players.c.name == player)
            r = c.execute(stmt).scalar()

            # insert player if not already in DB
            if r is None:
                stmt = (
                    insert(players).
                    values(name=player)
                )
                c.execute(stmt)

    def merge_team_player(self, team: str, player: str, year: int) -> None:
        """Insert team <-> player relation if not existent, else updates it"""
        with self.engine.connect() as c:
            stmt = (
                select(team_player.c.team_name).
                where(and_(team_player.c.player_name == player,
                           team_player.c.year == year))
            )
            r = c.execute(stmt).scalar()

            # insert relation if not already in DB
            if r is None:
                stmt = (
                    insert(team_player).
                    values(team_name=team, player_name=player, year=year)
                )
                c.execute(stmt)
            # change relation if it differs
            elif r != team:
                stmt = (
                    update(team_player).
                    where(and_(team_player.c.player_name == player,
                               team_player.c.year == year)).
                    values(team_name=team)
                )
                c.execute(stmt)
            else:
                # entry already correct
                pass

    def merge_player_salary(self, player: str, year: int, salary: int, salary_currency: str) -> None:
        """Insert player salary if not existent, else updates it"""
        with self.engine.connect() as c:
            stmt = (
                select(player_salaries.c.salary, player_salaries.c.salary_currency).
                where(and_(player_salaries.c.player_name == player,
                           player_salaries.c.year == year))
            )
            r = c.execute(stmt).one_or_none()

            # insert salary if not already in DB
            if r is None:
                stmt = (
                    insert(player_salaries).
                    values(player_name=player,
                           year=year,
                           salary=salary,
                           salary_currency=salary_currency)
                )

                c.execute(stmt)
            # change salary if it differs
            elif r[0] != salary or r[1] != salary_currency:
                stmt = (
                    update(player_salaries).
                    where(and_(player_salaries.c.player_name == player,
                               player_salaries.c.year == year)).
                    values(salary=salary, salary_currency=salary_currency)
                )
                c.execute(stmt)
            else:
                # entry already correct
                pass

    def merge_player_stats(self, player: str, year: int, stats: Dict[str, Union[str, float, int]]):
        """
        Insert player statistics for given year if not existent, else updates it

        The statistics must be one of the following types: 'str', 'float', 'int'.
        """
        def type2str(value):
            if type(value) == int:
                return 'Integer'
            elif type(value) == float:
                return 'Float'
            elif type(value) == str:
                return 'String'
            else:
                raise ValueError("Unexpected type for statistic")

        with self.engine.connect() as c:
            for stat, value in stats.items():
                stmt = (
                    select(player_stats.c.stat_value, player_stats.c.stat_type).
                    where(and_(player_stats.c.player_name == player,
                               player_stats.c.year == year,
                               player_stats.c.stat_name == stat))
                )
                r = c.execute(stmt).one_or_none()

                t = type2str(value)
                v = str(value)
                if r is None:
                    stmt = (
                        insert(player_stats).
                        values(player_name=player,
                               year=year,
                               season='postseason',
                               stat_name=stat,
                               stat_value=v,
                               stat_type=t)
                    )
                    c.execute(stmt)
                elif r[0] != v or r[1] != t:
                    stmt = (
                        update(player_stats).
                        where(and_(player_stats.c.player_name == player,
                                   player_stats.c.year == year,
                                   player_stats.c.stat_name == stat)).
                        values(stat_value=v, stat_type=t)
                    )
                    c.execute(stmt)
                else:
                    # entry already correct
                    pass

    def fetch_player_salary_playoffs(self, year: int, limit: Optional[int] = None) -> List[PlayerYearModel]:
        """
        Fetches all player salaries in teams participated in the playoffs in descending order from a specific `year`

        Args:
            year: Year for which the data is fetched
            limit: Maximal number of rows fetched, if None all rows are fetched
        """
        result = []

        with self.engine.connect() as c:
            stmt = (
                select(playoff_team.c.year, playoff_team.c.team_name, team_player.c.player_name,
                       player_salaries.c.salary, player_salaries.c.salary_currency).
                select_from(
                    playoff_team.
                    join(team_player, team_player.c.team_name == playoff_team.c.team_name).
                    join(player_salaries, and_(player_salaries.c.player_name == team_player.c.player_name,
                         player_salaries.c.year == team_player.c.year))
                ).
                where(playoff_team.c.year == year).
                order_by(player_salaries.c.salary.desc()).
                limit(limit)
            )

            r = c.execute(stmt)

            for row in r:
                att = {'salary': row[3], 'salary_currency': row[4]}
                result.append(PlayerYearModel(year=row[0], team=row[1], name=row[2], attributes=att))

        return result

    def fetch_player_stats(self, year: int, season: str) -> pd.DataFrame:
        """
        Return player statistics for given year and season as pandas DataFrame

        Args:
            year: Year for which the data is fetched
            season: Season for which the data is fetched
        """
        cols = ['PLAYER', 'STAT_NAME', 'STAT_VALUE', 'STAT_TYPE']
        df = pd.DataFrame(columns=cols)

        with self.engine.connect() as c:
            stmt = (
                select(player_stats.c.player_name, player_stats.c.stat_name,
                       player_stats.c.stat_value, player_stats.c.stat_type).
                where(and_(player_stats.c.year == year, player_stats.c.season == season))
            )
            r = c.execute(stmt).all()

            if not r:
                return df

            df = pd.DataFrame(r, columns=cols)

            def str2type(type_str, value_str):
                if type_str == 'Integer':
                    return np.int32(value_str)
                elif type_str == 'Float':
                    return np.float64(value_str)
                else:
                    return value_str

            # convert types according to stat type
            # if only Integer and Float types are present, all are transformed implicitly to float
            df['STAT_VALUE'] = df.apply(lambda x: str2type(x.STAT_TYPE, x.STAT_VALUE), axis=1)
            df = df.drop(columns=['STAT_TYPE'])
            df = df.pivot(index='PLAYER', columns=['STAT_NAME'])

            # remove indexed columns
            df.columns = df.columns.droplevel()
            df.columns.name = ''

        return df
