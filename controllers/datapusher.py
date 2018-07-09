### Purpose of file: This is the controller that will perform file data gathering and push to the db ###

## Imports
from controllers.ormcontroller import *
from controllers.datamunger import Munger

"""
### Needed Functions ###
Use the Harvest Param to only pull since last update, only pull things after that entry 
    - Until that, will just drop the table and re-insert everything
"""

##  Classes


class PusherBot(object):
    # This class handles collating and pushing clean data to the DB
    def __init__(self, is_test=False):
        if is_test:
            pass # Someday there will be test data to fill and rainbows and gumdrops will be everywhere
        else:
            # Instantiate our friendly data Munger to transform data
            self.mungy = Munger()

    def load_data(self, people=False, clients=False, tasks=False, projects=False, assignments=False,
                  time_entries=False, legacy_entries=False):
        # No graceful time diff right now, drop all data and create tables
        db.drop_all_tables(with_all_data=True)
        db.create_tables()
        """
        For each flag, check if enabled and then process if true
        """
        if people:
            self.munged_people = self.mungy.munge_person_list()
            print('Inserting People DB records')
            self.people_list = self.munged_people['people']
            insert_people_list(self.people_list)

        if clients:
            self.munged_clients = self.mungy.munge_client_list()
            print('Inserting Client DB records')
            self.client_list = self.munged_clients['clients']
            insert_clients_list(self.client_list)

        if tasks:
            self.munged_tasks = self.mungy.munge_task_list()
            print('Inserting Task DB records')
            self.tasks_list = self.munged_tasks['tasks']
            insert_tasks_list(self.tasks_list)

        if projects:
            self.munged_projects = self.mungy.munge_project_list()
            print('Inserting Project DB records')
            self.project_list = self.munged_projects['projects']
            insert_projects_list(self.project_list)

        if assignments:
            self.assignments_list = self.mungy.munge_forecast_assignments()
            print('Inserting Forecast Assignment DB records')
            insert_time_assignment_list(self.assignments_list)

        if time_entries:
            self.munged_time_entries = self.mungy.munge_harvest_time_entries()
            print('Inserting Time Entry DB records')
            self.time_entries_list = self.munged_time_entries['time_entries']
            insert_time_entries_list(self.time_entries_list)

        if legacy_entries:
            self.legacy_time_entries_list = self.mungy.munge_legacy_harvest_entries(self.time_entries_list)
            print('Inserting Legacy Harvest Entry DB records')
            insert_legacy_time_entries_list(self.legacy_time_entries_list)