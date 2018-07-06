"""
This file takes all API data, combines, transforms, and calculates it before it's ready to be sent off to the DB
It will greatly simplify the actions taken within the main.py file
"""

# Imports
from controllers.datapusher import DataActor
from controllers.datagrabber import Harvester, Forecaster
from data_rocket_conf import config as conf

# Vars
# grab config vars to pass to our classes later
forecast_account_id = conf['FORECAST_ACCOUNT_ID']
user_agent = conf['USER_AGENT']
auth_token = conf['HARVEST_AUTH']
harvest_account_id = conf['HARVEST_ACCOUNT_ID']
from_date = conf['FROM_DATE']

# Classes
# This class will transform the Harvest and Forecast Data
class Munger(object):

    def __init__(self, is_test=False):
        self.harv = Harvester(auth_token=auth_token, harvest_account_id=harvest_account_id,
                            user_agent=user_agent, is_test=is_test)

    # Do things like calculated column for total entry value
    def munge_harvest_time_entries(self):
        entries = self.harv.get_harvest_time_entries(from_date=from_date)

        # Cycle through each entry and perform needed transformations
        for entry in entries['time_entries']:
            # Calculate the total entry value
            if not entry['billable_rate']:
                entry.update(entry_amount=0)
            else:
                entry.update(entry_amount=(entry['hours'] * entry['billable_rate']))

        return entries

    # Munge the Forecast Assignments
    def munge_forecast_assignments(self,):
        pass

    # Combine the Harvest and Forecast User/People Lists, add fields, calculate fields
    def munge_person_list(self,):
        people = self.harv.get_harvest_users()

        for person in people['users']:
            # Fillers until goals are input
            person.update(weekly_goal=0)
            person.update(yearly_goal=0)


        return people

    # Combine the Harvest and Forecast Project Lists
    def munge_project_list(self,):
        projects = self.harv.get_harvest_projects()

        # Find projects that have no code and enter a zero
        for project in projects['projects']:
            if not project['code']:
                project.update(code=0)
            else:
                pass

        return projects

    # Get the Harvest Tasks List
    def munge_task_list(self,):
        tasks = self.harv.get_harvest_tasks()

        return tasks

    # Combine the Harvest and Forecast Client Lists
    def munge_client_list(self, ):
        clients = self.harv.get_harvest_clients()

        return clients