"""
This file takes all API data, combines, transforms, and calculates it before it's ready to be sent off to the DB
It will greatly simplify the actions taken within the main.py file
"""

# Imports
from controllers.datagrabber import Harvester, Forecaster
from controllers.ormcontroller import *
from datetime import datetime, timedelta
from numpy import is_busday


# Classes


# This class will transform the Harvest and Forecast Data into records for the Data Warehouse
class Munger(object):

    def __init__(self, is_test=False):
        self.harv = Harvester(is_test=is_test)
        self.fore = Forecaster(is_test=is_test)
        self.date_string = '%Y-%m-%d'
        self.mem_db = MemDB()

    """
    Utility Methods - These help keep code less reptitive
    """

    def __insert_harvest_id__(self, result_dict):
        # Insert Harvest ID, pop the old id key (Will be replaced by db identity key upon insert)

        if 'id' in result_dict:
            result_dict.update(harvest_id=result_dict.pop('id'))

        else:
            print('No id column found')

        return result_dict

    def __make_date_list__(self, start, end):
        # This takes two dates and provides the dates between them, inclusive of start and end
        date_string = self.date_string
        start_date = datetime.strptime(start, date_string)
        end_date = datetime.strptime(end, date_string)
        dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

        return dates

    def __find_orphan_forecast_ids__(self, harvest_list, forecast_list):
        # Searches two lists and finds which set of id's are missing
        harv_f_ids = []
        fore_ids = []
        for f_obj in forecast_list: # make a list of id's from the forecast list
            fore_ids.append(f_obj['id'])
        for h_obj in harvest_list: # make a list of id's from the harvest list
            if 'forecast_id' in h_obj:
                harv_f_ids.append(h_obj['forecast_id'])

        # Convert each list into a set, subtract them, convert back into list
        orphan_id_list = list(set(fore_ids) - set(harv_f_ids))
        orphan_list = []
        for o in orphan_id_list: # Loop through and find the dicts that match our orphans, return to method
            for f_obj in forecast_list:
                if f_obj['id'] == o:
                    orphan_list.append(f_obj)
        return orphan_list

    def __match_forecast_id__(self, harvest_obj, forecast_list):
        """
        Takes a single harvest object (Like a Project or Person dict) and checks if a corresponding value exists in a
        list of Forecast Objects
        :param harvest_obj: Dictionary with Harvest API Results
        :param forecast_list: List of Dictionaries with Forecast Results
        """
        for f_obj in (forecast_list):
            if f_obj['harvest_id'] == harvest_obj['harvest_id']:
                harvest_obj.update(forecast_id=f_obj['id'])
            else:
                pass

    def __refresh_memdb__(self, project=False, person=False, client=False):
        # Refresh tables for the memdb based on flags passed
        if project:
            self.mem_db.load_project_table()

        if person:
            self.mem_db.load_people_table()

        if client:
            self.mem_db.load_client_table()

    """
    Munge Methods
    The methods below modify their namesake data in ways needed to be useful for the data warehouse tool.
    """

    def munge_person_list(self):
        """
        This will pull the Harvest and Forecast user/people lists and combine them into one entry
        """
        harvest_people = self.harv.get_harvest_users()
        harvest_people['people'] = harvest_people.pop('users')
        forecast_people = self.fore.get_forecast_people()
        # Loop through each person and update fields as needed

        for person in harvest_people['people']:
            # Filler goals until goals are input
            person.update(weekly_goal=0)
            person.update(yearly_goal=0)
            # Update the Harvest ID
            self.__insert_harvest_id__(person)
            # Match the Harvest and Forecast Projects
            self.__match_forecast_id__(person, forecast_people['people'])\
            # Populate the full_name field
            try: #protect against bad key error if someone didn't have fields filled in on Harvest
                person.update(full_name=str(person['first_name'] + ' ' + person['last_name']))
            except Exception as e:
                print("""Couldn't make full name for {}.  Make sure that person is setup in Harvest
                      correctly""".format(person),e)

        # Find orphaned Forecast People
        orphan_f_list = self.__find_orphan_forecast_ids__(harvest_list=harvest_people['people'],
                                                          forecast_list=forecast_people['people'])

        # Insert orphaned People into our dict before returning
        if len(orphan_f_list) > 0:
            for o in orphan_f_list:
                # Get the Forecast information for each orphan, prepare for entry into the main dict
                o.update(forecast_id=o.pop('id'))
                harvest_people['people'].append(o)

        return harvest_people

    def munge_client_list(self):
        """
        Pulls the client list from Harvest and modifies the primary key for data warehouse
        """
        harvest_clients = self.harv.get_harvest_clients()
        forecast_clients = self.fore.get_forecast_clients()
        # Loop through each client and update the fields

        for client in harvest_clients['clients']:
            # update the Harvest ID column
            self.__insert_harvest_id__(client)

            # Match the Harvest and Forecast Projects
            self.__match_forecast_id__(client, forecast_clients['clients'])

        # Find orphaned Forecast Clients
        orphan_f_list = self.__find_orphan_forecast_ids__(harvest_list=harvest_clients['clients'],
                                                          forecast_list=forecast_clients['clients'])

        # Insert orphaned People into our dict before returning
        if len(orphan_f_list) > 0:
            for o in orphan_f_list:
                # Get the Forecast information for each orphan, prepare for entry into the main dict
                o.update(forecast_id=o.pop('id'))
                harvest_clients['clients'].append(o)

        return harvest_clients

    def munge_project_list(self):
        """
        Pulls both Harvest and Forecast project list so the two can be combined into one entity in data warehouse
        Also re-writes the primary keys to match data warehouse rather than the APIs
        """
        harvest_projects = self.harv.get_harvest_projects()
        forecast_projects = self.fore.get_forecast_projects()
        # Refresh the project and people tables before transforming data
        self.__refresh_memdb__(client=True)
        client_tbl = self.mem_db.client_tbl

        # Loop through each record and perform operations

        for project in harvest_projects['projects']:
            # Find projects that have no code and enter a zero
            if not project['code']:
                project.update(code=0)
            else:
                pass

            # Update the Harvest ID column so the data warehouse can assign a serial identiy
            self.__insert_harvest_id__(project)

            # Match the Harvest and Forecast Projects
            self.__match_forecast_id__(project, forecast_projects['projects'])

            # Update the Client ID to match the data warehouse ID - find by harvest or forecast id
            for client in client_tbl:
                if project['client_id'] == client.harvest_id or project['client_id'] == client.forecast_id:
                    project.update(client_id=client.id)

        # Find orphaned Forecast Projects
        orphan_f_list = self.__find_orphan_forecast_ids__(harvest_projects['projects'], forecast_projects['projects'])

        # Insert orphaned Projects into our dict before returning
        if len(orphan_f_list) > 0:
            for o in orphan_f_list:
                # Get the Forecast information for each orphan, prepare for entry into the main dict
                o.update(forecast_id=o.pop('id'))
                if o['client_id']: # If there is a client ID, update and append
                    dw_client = get_client_by_forecast_id(o['client_id'])
                    o.update(client_id=dw_client.id)
                    o.update(client_name=dw_client.name)
                else: # It's the Time Off Project, assign to RevUnit as Client ID and Name
                    o.update(client_id=164)
                    o.update(client_name='RevUnit')

                harvest_projects['projects'].append(o)

        return harvest_projects

    def munge_task_list(self):
        """
        Grabs tasks list from Harvest and returns them.  No transoformation needed.
        """
        tasks = self.harv.get_harvest_tasks()

        return tasks

    def munge_harvest_time_entries(self):
        """
         Transforms the time entries table into data warehouse data.
         Replaces the API identiy columns for project, client, and person with the data warehouse PKs
        """
        entries = self.harv.get_harvest_time_entries()
        # Refresh the project and people tables before transforming data
        self.__refresh_memdb__(project=True, person=True, client=True)
        ppl_tbl = self.mem_db.ppl_tbl
        proj_tbl = self.mem_db.proj_tbl
        client_tbl = self.mem_db.client_tbl

        # Cycle through each entry and perform needed transformations

        for entry in entries['time_entries']:
            # Scrub references to user_id and user_name
            entry.update(person_id=entry.pop('user_id'))
            entry.update(person_name=entry.pop('user_name'))

            # Calculate the total entry value
            if not entry['billable_rate']:
                entry.update(entry_amount=0)
            else:
                entry.update(entry_amount=(entry['hours'] * entry['billable_rate']))

            # Update the related table IDs with the Data Warehouse entries
            for person in ppl_tbl:
                if entry['person_id'] == person.harvest_id:
                    entry.update(person_id=person.id)
                else:
                    pass

            for project in proj_tbl:
                if entry['project_id'] == project.harvest_id:
                    entry.update(project_id=project.id)
                else:
                    pass

            for client in client_tbl:
                if entry['client_id'] == client.harvest_id:
                    entry.update(client_id=client.id)
                else:
                    pass

        return entries

    def munge_forecast_assignments(self):
        """
        Forecast API is very different than Harvest, so requires quite a bit of munging.
        Takes the date range from the Forecast entry and splits it into individual business day entries
        Calculates the hours/day for each assignment (source shows in seconds)
        Replaces API identity values with data warehouse ones
        """
        assignments = self.fore.get_forecast_assignments()
        date_string = self.date_string
        # Refresh the project and people tables before transforming data
        self.__refresh_memdb__(project=True, person=True)
        ppl_tbl = self.mem_db.ppl_tbl
        proj_tbl = self.mem_db.proj_tbl

        # Loop through each assignment and perform transformation

        assn_list = []
        errors = 0
        for assn in assignments['assignments']:
            # Get identity value, date range, and allocation value for the assignment
            id = assn.pop('id')
            start_date = assn.pop('start_date')
            end_date = assn.pop('end_date')
            try: # Protect against dividing non-floats and blowing up the app
                allocation = assn.pop('allocation') / 3600
            except Exception as e:
                errors += 1

            # Check against the memdb table stores for matching id's, update when found
            for proj in proj_tbl:
                if assn['project_id'] == proj.forecast_id:
                    assn.update(project_id=proj.id)

            for person in ppl_tbl:
                if assn['person_id'] == person.forecast_id:
                    assn.update(person_id=person.id)

            # Create the date list for the Forecast Assignment
            date_list = self.__make_date_list__(start_date, end_date)

            # For each business day in the assignment, make a new split entry
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

# This class will hold a pull of tables from the DB so they can be processed in memory on large loads
class MemDB(object):
    """
    Uses the get_*_table() methods from the ORM Controller
    I would like to replace this with a sqlite instance in memory to eliminate for loops when search two records
    """
    def __init__(self):
        self.proj_tbl = get_project_table()
        self.ppl_tbl = get_person_table()
        self.client_tbl = get_client_table()

    def load_project_table(self):
        self.proj_tbl = get_project_table()

    def load_people_table(self):
        self.ppl_tbl = get_person_table()

    def load_client_table(self):
        self.client_tbl = get_client_table()
