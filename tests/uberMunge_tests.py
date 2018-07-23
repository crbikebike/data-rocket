from nose.tools import *
from controllers.datamunger import UberMunge
from tests.mock_data.mock_harvest_apis import MockHarvester, MockForecaster

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

def test_munge_person():
    uber.munge_person()
    self.fail()

def test_munge_client():
    self.fail()

def test_munge_task():
    self.fail()

def test_munge_project():
    self.fail()

def test_munge_time_entries():
    self.fail()

def test_munge_assignment():
    self.fail()

def test_set_load_dates():
    self.fail()


get_mock_data(uber)
test_munge_person()