"""
This file takes all API data, combines, transforms, and calculates it before it's ready to be sent off to the DB
It will greatly simplify the actions taken within the main.py file
"""

# Imports
from controllers.datagrabber import Harvester, Forecaster
from data_rocket_conf import config as conf
from datetime import datetime

# Vars
# grab config vars to pass to our classes later
forecast_account_id = conf['FORECAST_ACCOUNT_ID']
user_agent = conf['USER_AGENT']
auth_token = conf['HARVEST_AUTH']
harvest_account_id = conf['HARVEST_ACCOUNT_ID']
from_date = conf['FROM_DATE']

"""
Note to self: Need to change the 'id' key value to the rootkey_id in each table before sending off
"""

# Classes
# This class will transform the Harvest and Forecast Data
class Munger(object):

    def __init__(self, is_test=False):
        self.harv = Harvester(auth_token=auth_token, harvest_account_id=harvest_account_id,
                            user_agent=user_agent, is_test=is_test)
        self.fore = Forecaster(forecast_account_id=forecast_account_id, user_agent=user_agent,
                               auth_token=auth_token, is_test=is_test)

    def __insert_harvest_id__(self, result_dict):
        # Insert Harvest ID, pop the old id key (Will be replaced by db identity key upon insert)

        if 'id' in result_dict:
            result_dict.update(harvest_id=result_dict.pop('id'))

        else:
            print('No id column found')

        return result_dict

    """
    The functions below modify data in ways needed to be useful for the BI tool.
    """

    def munge_person_list(self):
        people = self.harv.get_harvest_users()
        people['people'] = people.pop('users')

        # Loop through each person and update fields as needed

        for person in people['people']:
            # Filler goals until goals are input
            person.update(weekly_goal=0)
            person.update(yearly_goal=0)

            # Update the Harvest ID
            self.__insert_harvest_id__(people)

        return people

    def munge_client_list(self):
        clients = self.harv.get_harvest_clients()

        # Loop through each client and update the fields

        for client in clients['clients']:

            # update the Harvest ID column
            self.__insert_harvest_id__(client)

        return clients

    def munge_project_list(self):
        projects = self.harv.get_harvest_projects()

        # Find projects that have no code and enter a zero
        for project in projects['projects']:
            if not project['code']:
                project.update(code=0)
            else:
                pass

            # Update the Harvest ID column
            self.__insert_harvest_id__(project)

        return projects

    def munge_task_list(self):
        tasks = self.harv.get_harvest_tasks()

        return tasks

    def munge_harvest_time_entries(self):
        entries = self.harv.get_harvest_time_entries(from_date=from_date)

        # Cycle through each entry and perform needed transformations

        for entry in entries['time_entries']:
            # Calculate the total entry value
            if not entry['billable_rate']:
                entry.update(entry_amount=0)
            else:
                entry.update(entry_amount=(entry['hours'] * entry['billable_rate']))
            # Find the harvest_id in the people table and then change to the identiy key from

        return entries

    # Munge the Forecast Assignments
    def munge_forecast_assignments(self,):
        assignments = self.fore.get_forecast_assignments()
        date_string = '%Y-%m-%d'

        # Forecast Assignments require to be calculated in hrs/d and number of days
        # so they can be shown in weekly buckets

        for assignment in assignments:
            start_date = datetime.strptime(assignment['start_date'], date_string)
            end_date = datetime.strptime(assignment['end_date'], date_string)
            assignment_length = (end_date - start_date).days + 1
            allocation_hours = assignment['allocation'] / 3600
            allocation_conv = assignment_length * allocation_hours

            assignment.update(allocation=allocation_conv)
