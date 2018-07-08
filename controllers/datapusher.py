### Purpose of file: This is the controller that will perform file data gathering and push to the db ###

## Imports
from controllers.ormcontroller import *
from controllers.datamunger import Munger

"""
### Needed Functions ###

Find existing rows, update if needed - probably only look back 30 days since the entries get locked

Use the Harvest Param to only pull since last update, only pull things after that entry 
    - Until that, will just drop the table and re-insert everything

Count rows being inserted - give status when, catch error and note where it left off
"""

##  Classes

# This class handles collating and pushing clean data to the DB
class PusherBot(object):

    def __init__(self, is_test=False):
        if is_test:
            pass
        else:
            # Instantiate our friendly data Munger to ready data
            self.mungy = Munger()

    # Call appropriate functions to get munged data, return tuple of data dicts
    def load_data(self, people=False, clients=False, tasks=False, projects=False, assignments=False,
                  time_entries=False):

        """
        For each flag, check if enabled and then process if true
        """
        if people:
            people = self.mungy.munge_person_list()
            print('Inserting People DB records')
            people_list = people['people']
            insert_people_list(people_list)

        if clients:
            clients = self.mungy.munge_client_list()
            print('Inserting Client DB records')
            client_list = clients['clients']
            insert_clients_list(client_list)

        if tasks:
            tasks = self.mungy.munge_task_list()
            print('Inserting Task DB records')
            tasks_list = tasks['tasks']
            insert_tasks_list(tasks_list)

        if projects:
            projects = self.mungy.munge_project_list()
            print('Inserting Project DB records')
            project_list = projects['projects']
            insert_projects_list(project_list)

        if assignments:
            assignments = self.mungy.munge_forecast_assignments()
            print('Inserting Forecast Assignment DB records')
            assignments = assignments['assignments']
            insert_time_assignment_list(assignments)

        if time_entries:
            time_entries = self.mungy.munge_harvest_time_entries()
            print('Inserting Time Entry DB records')
            time_entries = time_entries['time_entries']
            insert_time_entries_list(time_entries)
