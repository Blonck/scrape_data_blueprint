import unittest

from scraping.db import DbHandler, teams
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
