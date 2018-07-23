import unittest
from controllers.datamunger import UberMunge
from tests.mock_data.mock_harvest_apis import MockHarvester, MockForecaster
from controllers.ormcontroller import db, db_session, select
from controllers.ormobjects import *

mock_datetime = '1984-12-31T00:00:00Z'

"""
Setup #FakeData
"""

uber = UberMunge(is_test=True)

def get_mock_data(munger):
    # Call the mock classes that will return dummy data
    harv = MockHarvester()
    fore = MockForecaster()

    # Replace attributes that would hav been set in the __init__ function
    munger.set_load_dates(is_full_load=True)
    munger.time_entry_last_updated = mock_datetime
    munger.harv = harv
    munger.fore = fore

"""
Perform Tests
"""


@db_session
def test_munge_person():
    uber.munge_person()
    peeps = select(p for p in Person)[:]
    assert len(peeps) > 0

def test_munge_client():
    pass

def test_munge_task():
    pass

def test_munge_project():
    pass

def test_munge_time_entries():
    pass

def test_munge_assignment():
    pass

def test_set_load_dates():
    pass


get_mock_data(uber)
test_munge_person()