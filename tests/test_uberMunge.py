from unittest import TestCase, main as utmain
from controllers.datamunger import UberMunge
from tests.mock_data.mock_harvest_apis import MockHarvester, MockForecaster
from controllers.ormcontroller import db, db_session, select
from controllers.ormobjects import *

mock_datetime = '1984-12-31T00:00:00Z'

class TestUberMunge(TestCase):
    """Tests for the critical transformations done by UberMunger"""

    def setUp(self):
        """Create clean environment before each test"""
        db.drop_all_tables(with_all_data=True)
        db.create_tables()
        self.uber = UberMunge(is_test=True)

        # Call the mock classes that will return dummy data
        harv = MockHarvester()
        fore = MockForecaster()

        # Replace attributes that would hav been set in the __init__ function
        self.uber.set_load_dates(is_full_load=True)
        self.uber.time_entry_last_updated = mock_datetime
        self.uber.harv = harv
        self.uber.fore = fore

    def tearDown(self):
        self.uber.harv = None
        self.uber.fore = None

    @db_session
    def test_munge_person_single_role(self):
        """Tests a person with a Forecast ID and a single role in Harvest"""
        self.uber.munge_person()
        peeps = select(p for p in Person)[:]
        p1 = Person.get(harvest_id=1111111)

        # Test the transform operations that happen in the munge_person
        self.assertTrue(len(peeps) > 0)
        self.assertEqual(p1.full_name, 'John Doe')
        self.assertEqual(p1.roles, 'Technology')
        self.assertEqual(p1.forecast_id, 711111)

    @db_session
    def test_munge_person_multi_role(self):
        """Tests a person with multiple roles in Harvest"""
        self.uber.munge_person()
        peeps = select(p for p in Person)[:]
        p1 = Person.get(harvest_id=2222222)

        # Test the transform operations that happen in the munge_person
        self.assertTrue(len(peeps) > 0)
        self.assertEqual(p1.full_name, 'CJ Designerperson')
        self.assertEqual(p1.roles, 'Design (Ops)')
        self.assertEqual(p1.forecast_id, 722222)

    @db_session
    def test_munge_person_multi_role_exec(self):
        """Tests a person with multiple roles in Harvest that is a member of executive team"""
        self.uber.munge_person()
        peeps = select(p for p in Person)[:]
        p1 = Person.get(harvest_id=3333333)

        # Test the transform operations that happen in the munge_person
        self.assertTrue(len(peeps) > 0)
        self.assertEqual(p1.full_name, 'Chris Technologist')
        self.assertEqual(p1.roles, 'Exec')

    @db_session
    def test_munge_person_multi_role_missioncontrol(self):
        """Tests a person with multiple roles in Harvest that is a member of mission control"""
        self.uber.munge_person()
        peeps = select(p for p in Person)[:]
        p1 = Person.get(harvest_id=4444444)

        # Test the transform operations that happen in the munge_person
        self.assertTrue(len(peeps) > 0)
        self.assertEqual(p1.full_name, 'Tony VPDude')
        self.assertEqual(p1.roles, 'Mission Control')

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

if __name__ == '__main__':
    utmain()