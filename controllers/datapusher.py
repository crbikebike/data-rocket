### Purpose of file: This is the controller that will perform file data gathering and push to the db ###

## Imports
import controllers.ormcontroller as orm
from controllers.datamunger import Munger

"""
### Needed Functions ###

Find existing rows, update if needed - probably only look back 30 days since the entries get locked

Use the Harvest Param to only pull since last update, only pull things after that entry 
    - Until that, will just drop the table and re-insert everything

Count rows being inserted - give status when, catch error and note where it left off
"""

##  Classes
# This class needs to be remade to take the clutter from the main.py file
class PusherBot(object):

    def __init__(self, is_test=False):
        if is_test:
            pass
        else:
            # Instantiate our friendly data Munger to ready data
            self.mungy = Munger()

    # Call appropriate functions to get munged data, return tuple of data dicts
    def load_data(self):

        """
        Note to self - need to add if statement for full load or differential
        """

        # Place Munged Harvest entries into a dictionary
        time_entries = self.mungy.munge_harvest_time_entries()
        people = self.mungy.munge_person_list()
        clients = self.mungy.munge_client_list()
        tasks = self.mungy.munge_task_list()
        projects = self.mungy.munge_project_list()



        return [people, clients, projects, tasks, time_entries]

    # Push data out to the db
    def push_data(self, loaded_data):
        # Loop through our loaded entry and insert what we get
        for data_list in loaded_data:
            if 'people' in data_list:
                people = data_list
                print('Inserting People DB records')
                people_list = people['people']
                orm.insert_people_list(people_list)

            elif 'clients' in data_list:
                clients = data_list
                print('Inserting Client DB records')
                client_list = clients['clients']
                orm.insert_clients_list(client_list)

            elif 'projects' in data_list:
                projects = data_list
                print('Inserting Project DB records')
                project_list = projects['projects']
                orm.insert_projects_list(project_list)

            elif 'tasks' in data_list:
                tasks = data_list
                print('Inserting Task DB records')
                tasks_list = tasks['tasks']
                orm.insert_tasks_list(tasks_list)

            elif 'time_entries' in data_list:
                time_entries = data_list
                print('Inserting Time Entry DB records')
                time_entries = time_entries['time_entries']
                orm.insert_time_entries_list(time_entries)