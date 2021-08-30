import unittest

from scraping.db import DbHandler, teams, playoff_team, team_player
from sqlalchemy import *


class TestDbHandler(unittest.TestCase):
    def setUp(self):
        # intialize DB for each test
        self.db = DbHandler('sqlite:///:memory:')

    def test_fetch_player_salary_playoffs_empty(self):
        """Test that fetching player salaries on empyt DB returns empty array"""
        r = self.db.fetch_player_salary_playoffs(year=2000)
        self.assertEqual(r, [])

    def test_fetch_player_stats_empty(self):
        """Test that fetching player statistics on empyt DB returns empty DataFrame"""
        r = self.db.fetch_player_stats(year=2000, season='postseason')

        cols = ['PLAYER', 'STAT_NAME', 'STAT_VALUE', 'STAT_TYPE']
        self.assertEqual(list(r.columns), cols)
        self.assertTrue(r.empty)

    def get_teams(self):
        with self.db.engine.connect() as c:
            stmt = select(teams)
            return c.execute(stmt).all()

    def get_playoff_team(self):
        with self.db.engine.connect() as c:
            stmt = select(playoff_team.c.year, playoff_team.c.team_name)
            return c.execute(stmt).all()

    def get_team_player(self):
        with self.db.engine.connect() as c:
            stmt = select(team_player.c.player_name, team_player.c.team_name, team_player.c.year)
            return c.execute(stmt).all()

    def test_merge_team(self):
        # DB is empty, no teams
        self.assertEqual(self.get_teams(), [])

        # add a Team
        self.db.merge_team('TeamA')
        self.assertEqual(self.get_teams(), [('TeamA', )])

        # add same team again, nothing changes
        self.db.merge_team('TeamA')
        self.assertEqual(self.get_teams(), [('TeamA', )])

        # add second team
        self.db.merge_team('TeamB')
        self.assertEqual(self.get_teams(), [('TeamA', ), ('TeamB', )])

    def test_merge_playoff_team(self):
        # DB is empty, no playoff teams
        self.assertEqual(self.get_playoff_team(), [])

        # add playoff team
        self.db.merge_playoff_team('TeamA', 2020)
        self.assertEqual(self.get_playoff_team(), [(2020, 'TeamA')])

        # add same team and year again, nothing changes
        self.db.merge_playoff_team('TeamA', 2020)
        self.assertEqual(self.get_playoff_team(), [(2020, 'TeamA')])

        self.db.merge_playoff_team('TeamA', 2021)
        self.assertEqual(sorted(self.get_playoff_team()),
                         sorted([(2020, 'TeamA'), (2021, 'TeamA')]))

        self.db.merge_playoff_team('TeamB', 2021)
        self.assertEqual(sorted(self.get_playoff_team()),
                         sorted([(2020, 'TeamA'), (2021, 'TeamA'), (2021, 'TeamB')]))

    def test_merge_team_player(self):
        # DB is empty, no team player relations
        self.assertEqual(self.get_team_player(), [])

        # add relation
        tp01 = ('PlayerA', 'TeamA', 2021)
        self.db.merge_team_player(player=tp01[0], team=tp01[1], year=tp01[2])
        self.assertEqual(self.get_team_player(), [tp01])

        # add same relation again, nothing changes
        self.db.merge_team_player(player=tp01[0], team=tp01[1], year=tp01[2])
        self.assertEqual(self.get_team_player(), [tp01])

        # change team for same player and year -> still only one entry
        tp01 = ('PlayerA', 'TeamB', 2021)
        self.db.merge_team_player(player=tp01[0], team=tp01[1], year=tp01[2])
        self.assertEqual(self.get_team_player(), [tp01])

        # add second relation
        tp02 = ('PlayerB', 'TeamB', 2021)
        self.db.merge_team_player(player=tp02[0], team=tp02[1], year=tp02[2])
        self.assertEqual(sorted(self.get_team_player()),
                         sorted([tp01, tp02]))

        # add third relation with different year
        tp03 = ('PlayerB', 'TeamB', 2019)
        self.db.merge_team_player(player=tp03[0], team=tp03[1], year=tp03[2])
        self.assertEqual(sorted(self.get_team_player()),
                         sorted([tp01, tp02, tp03]))
