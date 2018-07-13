"""
This file takes all API data, combines, transforms, and calculates it before it's ready to be sent off to the DB
It will greatly simplify the actions taken within the main.py file
"""

# Imports
from controllers.datagrabber import Harvester, Forecaster
from controllers.ormcontroller import *
from datetime import datetime, timedelta
from numpy import is_busday
from data_rocket_conf import config as conf

# Classes

class Munger(object):
    """
    This class will transform the Harvest and Forecast Data into records for the Data Warehouse
    """
    def __init__(self, is_test=False):
        self.harv = Harvester(is_test=is_test)
        self.fore = Forecaster(is_test=is_test)
        self.date_string = '%Y-%m-%d'
        self.datetime_string = '%Y-%m-%dT%H:%M:%SZ'
        self.mem_db = MemDB()
        self.last_updated_dict = get_updated_from_dates()
        self.full_load_datetime = '2010-01-01T00:00:00Z'


    """
    Utility Methods - These help keep code less reptitive
    """

    def set_load_dates(self, is_full_load):
        if is_full_load:
            self.person_last_updated = self.full_load_datetime
            self.project_last_updated = self.full_load_datetime
            self.client_last_updated = self.full_load_datetime
            self.task_last_updated = self.full_load_datetime
            self.time_entry_last_updated = conf['FROM_DATE']
        else:
            self.person_last_updated = self.last_updated_dict['person'].strftime(self.datetime_string)
            self.project_last_updated = self.last_updated_dict['project'].strftime(self.datetime_string)
            self.client_last_updated = self.last_updated_dict['client'].strftime(self.datetime_string)
            self.task_last_updated = self.last_updated_dict['task'].strftime(self.datetime_string)
            self.time_entry_last_updated = self.last_updated_dict['time_entry'].strftime(self.datetime_string)

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

    def __trim_forecast_results__(self, f_result_set, trim_datetime):
        """
        This will trip the result set down to the updated_since date
        """
        keeper_list = []
        for f_obj in f_result_set:
            if f_obj['updated_at'] > trim_datetime:
                keeper_list.append(f_obj)
            else:
                pass
        return keeper_list

    """
    Munge Methods
    The methods below modify their namesake data in ways needed to be useful for the data warehouse tool.
    """

    def munge_person_list(self):
        """
        This will pull the Harvest and Forecast user/people lists and combine them into one entry
        """
        harvest_people = self.harv.get_harvest_users(updated_since=self.person_last_updated)
        harvest_people['people'] = harvest_people.pop('users')
        forecast_people = self.fore.get_forecast_people()
        # Replace the full Forecast list with a trimmed one based on updated_at date
        forecast_people['people'] = self.__trim_forecast_results__(forecast_people['people'],self.person_last_updated)
        # Loop through each person and update fields as needed

        person_keepers = [] # If you like them, put a ring on them
        for person in harvest_people['people']:
            if person['id'] not in self.mem_db.people_ids:
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
                person_keepers.append(person)
            else:
                pass # One day will be an update function

        # Overwrite person list with the keeper list
        harvest_people.update(people=person_keepers)

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
        harvest_clients = self.harv.get_harvest_clients(updated_since=self.client_last_updated)
        forecast_clients = self.fore.get_forecast_clients()
        # Replace the full Forecast list with a trimmed one based on updated_at date
        forecast_clients['clients'] = self.__trim_forecast_results__(forecast_clients['clients'],
                                                                     self.client_last_updated)
        # Loop through each client and update the fields
        client_keepers = []
        for client in harvest_clients['clients']:
            if client['id'] not in self.mem_db.client_ids:
                # update the Harvest ID column
                self.__insert_harvest_id__(client)

                # Match the Harvest and Forecast Projects
                self.__match_forecast_id__(client, forecast_clients['clients'])
                client_keepers.append(client)

        # Overwrite the client list with the keepers
        harvest_clients.update(clients=client_keepers)

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
        harvest_projects = self.harv.get_harvest_projects(updated_since=self.project_last_updated)
        forecast_projects = self.fore.get_forecast_projects()
        # Replace the full Forecast list with a trimmed one based on updated_at date
        forecast_projects['projects'] = self.__trim_forecast_results__(forecast_projects['projects'],
                                                                       self.project_last_updated)
        # Refresh the client table before transforming data
        self.__refresh_memdb__(client=True)
        client_tbl = self.mem_db.client_tbl

        # Loop through each record and perform operations
        project_keepers = []
        for project in harvest_projects['projects']:
            if project['id'] not in self.mem_db.project_ids:
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
                project_keepers.append(project)
            else:
                pass
        # Overwrite the project list with the keepers
        harvest_projects.update(projects=project_keepers)

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
        tasks = self.harv.get_harvest_tasks(updated_since=self.task_last_updated)

        return tasks

    def munge_harvest_time_entries(self):
        """
         Transforms the time entries table into data warehouse data.
         Replaces the API identiy columns for project, client, and person with the data warehouse PKs
        """
        entries = self.harv.get_harvest_time_entries(updated_since=self.time_entry_last_updated)
        # Refresh the project and people tables before transforming data
        self.__refresh_memdb__(project=True, person=True, client=True)
        ppl_tbl = self.mem_db.ppl_tbl
        proj_tbl = self.mem_db.proj_tbl
        client_tbl = self.mem_db.client_tbl
        te_tbl = self.mem_db.te_tbl

        entry_keepers = [] # If you like it, put a ring on it.
        for entry in entries['time_entries']:
            # Cycle through each entry and perform needed transformations
            if entry['id'] not in self.mem_db.time_entry_ids:
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
                entry_keepers.append(entry)

        entries.update(time_entries=entry_keepers) # Overwrite the entry list with the one we munged
        return entries

    def munge_legacy_harvest_entries(self, harvest_entry_list):
        """
        This allows for filling legacy Entries table while reports are migrated to new format
        Update the entry list of dicts with the legacy fields so the data pusher can send to legacy table
        """
        legacy_entry_list = harvest_entry_list.copy()

        for entry in legacy_entry_list:
            entry.update(entry_id=entry.pop('id'))
            entry.update(user_id=entry.pop('person_id'))
            entry.update(user_name=entry.pop('person_name'))
            entry.update(harvest_project_id=entry.pop('project_id'))
            entry.update(harvest_project_name=entry.pop('project_name'))
            entry.update(harvest_project_code=entry.pop('project_code'))
        return legacy_entry_list

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
        self.te_tbl = get_time_entry_table()
        self.time_entry_ids = [te.id for te in self.te_tbl]
        self.people_ids = [p.harvest_id for p in self.ppl_tbl]
        self.project_ids = [prj.harvest_id for prj in self.proj_tbl]
        self.client_ids = [c.harvest_id for c in self.client_tbl]

    def load_project_table(self):
        self.proj_tbl = get_project_table()

    def load_people_table(self):
        self.ppl_tbl = get_person_table()

    def load_client_table(self):
        self.client_tbl = get_client_table()

    def load_time_entry_table(self):
        self.te_tbl = get_time_entry_table()


"""
The Munge 2.0 Class below
"""

class UberMunge(object):
    """
    This class transforms data before sending off to the DB
    """
    def __init__(self, is_test=False):
        self.harv = Harvester(is_test=is_test)
        self.fore = Forecaster(is_test=is_test)
        self.date_string = '%Y-%m-%d'
        self.datetime_string = '%Y-%m-%dT%H:%M:%SZ'
        self.last_updated_dict = get_updated_from_dates()
        self.full_load_datetime = '2010-01-01T00:00:00Z'

    """
    Utility Methods
    These help keep code less reptitive
    """

    def set_load_dates(self, is_full_load):
        if is_full_load:
            self.person_last_updated = self.full_load_datetime
            self.project_last_updated = self.full_load_datetime
            self.client_last_updated = self.full_load_datetime
            self.task_last_updated = self.full_load_datetime
            self.time_entry_last_updated = conf['FROM_DATE']
        else:
            self.person_last_updated = self.last_updated_dict['person'].strftime(self.datetime_string)
            self.project_last_updated = self.last_updated_dict['project'].strftime(self.datetime_string)
            self.client_last_updated = self.last_updated_dict['client'].strftime(self.datetime_string)
            self.task_last_updated = self.last_updated_dict['task'].strftime(self.datetime_string)
            self.time_entry_last_updated = self.last_updated_dict['time_entry'].strftime(self.datetime_string)


    """
    Munge Functions
    All these functions take in data, transform as needed, and push to the db
    """


    @db_session
    def mung_person(self):
        pass

    @db_session
    def munge_task(self):
        """Get all Harvest Tasks and send them to the db.

        :return:
        None
        """

        # Get the Harvest Tasks List
        harvest_tasks = self.harv.get_harvest_tasks(updated_since=self.task_last_updated)
        harvest_tasks_list = harvest_tasks['tasks']

        for task in harvest_tasks_list:
            # If a task is already in the DB, update it.  Otherwise insert it.

            t_id = task['id']
            if Task.get(id=t_id):
                Task[t_id].set(**task)
            else:
                Task(id=task['id'], name=task['name'], updated_at=task['updated_at'])

            # Commit the record to the db
            commit()