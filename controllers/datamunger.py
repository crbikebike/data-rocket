"""
This file takes all API data, combines, transforms, and calculates it before it's ready to be sent off to the DB
It will greatly simplify the actions taken within the main.py file
"""

# Imports
from controllers.datagrabber import Harvester, Forecaster
from data_rocket_conf import config as conf
from datetime import datetime, timedelta
from numpy import is_busday

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
        self.date_string = '%Y-%m-%d'

    def __insert_harvest_id__(self, result_dict):
        # Insert Harvest ID, pop the old id key (Will be replaced by db identity key upon insert)

        if 'id' in result_dict:
            result_dict.update(harvest_id=result_dict.pop('id'))

        else:
            print('No id column found')

        return result_dict

    # This takes two dates and provides the dates between them, inclusive of start and end
    def __make_date_list__(self, start, end):
        date_string = self.date_string
        start_date = datetime.strptime(start, date_string)
        end_date = datetime.strptime(end, date_string)
        dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

        return dates

    # Searches two dictionaries and inserts the forecast id for the entity - used for People and Projects
    def __insert_forecast_id__(self, harvest_list, forecast_list):

        for fperson in forecast_list:
            for hperson in harvest_list:
                if fperson['harvest_id'] == hperson['harvest_id']:
                    hperson.update(forecast_id=fperson['id'])
                else:
                    pass

    """
    The functions below modify data in ways needed to be useful for the BI tool.
    """

    def munge_person_list(self):
        """
        This will pull the Harvest and Forecast user/people lists and combine them into one entry
        """
        people = self.harv.get_harvest_users()
        people['people'] = people.pop('users')

        # Loop through each person and update fields as needed

        for person in people['people']:
            # Filler goals until goals are input
            person.update(weekly_goal=0)
            person.update(yearly_goal=0)

            # Update the Harvest ID
            self.__insert_harvest_id__(person)

        forecast_people = self.fore.get_forecast_people()

        # Insert the Forecast ID now that we have both lists
        self.__insert_forecast_id__(harvest_list=people['people'], forecast_list=forecast_people['people'])

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

        forecast_projects = self.fore.get_forecast_projects()

        # Insert the Forecast Project ID now that we have both lists
        self.__insert_forecast_id__(harvest_list=projects['projects'], forecast_list=forecast_projects['projects'])

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
        date_string = self.date_string

        # Forecast Assignments require hrs/day to be calculated and have the exact dates drawn from a range given

        assn_list = []
        for assn in assignments['assignments']:
            errors = 0
            id = assn.pop('id')
            start_date = assn.pop('start_date')
            end_date = assn.pop('end_date')
            try:
                allocation = assn.pop('allocation') / 3600
            except Exception as e:
                errors += 1


            # Create the date list for the Forecast Assignment
            date_list = self.__make_date_list__(start_date, end_date)

            # For each day in the assignment, make an entry
            for day in date_list:
                if is_busday(day):
                    split_assn = assn.copy()
                    split_assn.update(parent_id=id)
                    split_assn.update(assign_date=(day).strftime(date_string))
                    split_assn.update(allocation=allocation)
                    assn_list.append(split_assn)
                else:
                    pass

        print("Total errors while loading Forecast Assignments: {}".format(errors))

        return assn_list