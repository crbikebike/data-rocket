from nose.tools import *
from controllers.datamunger import UberMunge
from controllers.datagrabber import Harvester, Forecaster
from tests.mock_data import mock_harvester

"""
Setup #FakeData
"""

def get_mock_data():
    harv = Harvester()
    fore = Forecaster()
    uber = UberMunge()


    uber.last_updated_dict = {''}
    uber.harv = harv
    uber.fore = fore


"""
Perform Tests
"""


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

def test_set_load_dates(self):
    self.fail()
