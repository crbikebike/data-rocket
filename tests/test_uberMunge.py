from unittest import TestCase
from controllers.datamunger import UberMunge
from tests.mock_data.mock_harvest_apis import MockHarvester, MockForecaster
from controllers.ormcontroller import db, db_session, select
from controllers.ormobjects import *

mock_datetime = '1984-12-31T00:00:00Z'

class TestUberMunge(TestCase):
    def test_munge_person(self):
        self.fail()

    def test_munge_client(self):
        self.fail()

    def test_munge_task(self):
        self.fail()

    def test_munge_project(self):
        self.fail()

    def test_munge_time_entries(self):
        self.fail()

    def test_munge_assignment(self):
        self.fail()
