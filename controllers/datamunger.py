"""
This file takes all API data, combines, transforms, and calculates it before it's ready to be sent off to the DB
It will greatly simplify the actions taken within the main.py file
"""

# Imports
from controllers.datagrabber import Harvester, Forecaster
from controllers.ormcontroller import *
from controllers.utilitybot import datetime_string, date_string, datetime_str_ms, logger
from datetime import datetime, timedelta
from numpy import is_busday
from data_rocket_conf import config as conf

# Classes
class UberMunge(object):
    """This class transforms data before sending off to the DB for insert or update

    It is replacing the Munger class, which has proven buggy in diff loads and is overly complicated.  The general principle
    of UberMunge is to push data to the db immediately after transformation as an insert or update rather than load in a
    bulk transaction after all records are transformed.
    """
    def __init__(self, is_test=False):
        self.harv = Harvester(is_test=is_test)
        self.fore = Forecaster(is_test=is_test)
        self.last_updated_dict = get_updated_from_dates()
        self.full_load_datetime = '2010-01-01T00:00:00Z'

    """
    Munge Functions
    
    All these functions take in data, transform as needed, and push to the db
    """

    @db_session
    def munge_person(self):
        """Get all Harvest and Forecast people, combine, transform, and push them to db

        :return:
        None
        """
        # Get Harvest and Forecast people
        updated_since = self.person_last_updated
        harvest_people = self.harv.get_harvest_users(updated_since=updated_since)
        harvest_people_list = harvest_people['users']
        forecast_people = self.fore.get_forecast_people()
        forecast_people_list = forecast_people['people']
        # Trim the Forecast list to the updated_date
        forecast_people_list = self.__trim_forecast_results__(f_result_set=forecast_people_list,
                                                              trim_datetime=updated_since)

        # Update Primary Key, Get Forecast id for each person
        for h_person in harvest_people_list:
            h_person.update(harvest_id=h_person.pop('id'))
            h_person.update(forecast_id=None)
            # Convert the datetime strings into python datetime objects so the ORM can use them
            h_person.update(created_at=datetime.strptime(h_person['created_at'], datetime_string))
            h_person.update(updated_at=datetime.strptime(h_person['updated_at'], datetime_string))

            for idx, f_person in enumerate(forecast_people_list):
                if h_person['harvest_id'] == f_person['harvest_id']:
                    h_person.update(forecast_id=f_person['id'])
                    forecast_people_list.pop(idx)
                else:
                    pass

        # For each Person record, check if in db and then insert/update accordingly
        print('Writing People:')
        logger.print_progress_bar(iteration=0, total=len(harvest_people_list))
        for idx, person in enumerate(harvest_people_list):
            harvest_id = person['harvest_id']
            full_name = "{fn} {ln}".format(fn=person['first_name'], ln=person['last_name'])

            # If a Person is in db update, otherwise insert
            p = Person.get(harvest_id=harvest_id)
            if p:
                p.set(**person)
            else:
                np = Person(harvest_id=harvest_id,
                            forecast_id=person['forecast_id'],
                            first_name=person['first_name'],
                            last_name=person['last_name'],
                            full_name=full_name,
                            email=person['email'],
                            timezone=person['timezone'],
                            weekly_capacity=person['weekly_capacity'],
                            is_contractor=person['is_contractor'],
                            is_active=person['is_active'],
                            roles=person['roles'],
                            avatar_url=person['avatar_url'],
                            created_at=person['created_at'],
                            updated_at=person['updated_at'])
            # Commit the record
            db.commit()
            logger.print_progress_bar(iteration=idx + 1,total=len(harvest_people_list))

        # Cycle through remaining Forecast people to update forecast_id, if needed
        for f_person in forecast_people_list:
            f_person.update(forecast_id=f_person.pop('id'))
            full_name = "{fn} {ln}".format(fn=f_person['first_name'], ln=f_person['last_name'])
            is_active = not f_person.pop('archived')
            f_person.update(is_active=is_active)
            f_person.update(updated_at=datetime.strptime(f_person['updated_at'], datetime_str_ms))

            if f_person['harvest_id']:
                p = Person.get(harvest_id=f_person['harvest_id'])
                p.forecast_id = f_person['forecast_id']
            else:
                # If orphan Person exists, update. Else, insert.
                fp = Person.get(forecast_id=f_person['forecast_id'])
                if fp:
                    fp.set(**f_person)
                else:
                    nfp = Person(forecast_id=f_person['forecast_id'],
                                 first_name=f_person['first_name'],
                                 last_name=f_person['last_name'],
                                 full_name=full_name,
                                 email=f_person['email'],
                                 is_active=f_person['is_active'],
                                 updated_at=f_person['updated_at'])
            # Commit the records
            db.commit()

    @db_session
    def munge_client(self):
        """Pulls Harvest and Forecast Clients and inserts/updates records

        :return:
        """
        updated_since = self.client_last_updated
        harvest_clients = self.harv.get_harvest_clients(updated_since=updated_since)
        harvest_client_list = harvest_clients['clients']
        forecast_clients = self.fore.get_forecast_clients()
        forecast_clients_list = forecast_clients['clients']
        # Trim Forecast list based on updated_since var
        forecast_clients_list = self.__trim_forecast_results__(f_result_set=forecast_clients_list,
                                                               trim_datetime=updated_since)

        # Update Primary Key, get Forecast ID for each record
        for h_client in harvest_client_list:
            h_client.update(harvest_id=h_client.pop('id'))
            h_client.update(forecast_id=None)
            # Convert the date keys into Python date objects so ORM can use them
            h_client.update(created_at=datetime.strptime(h_client['created_at'], datetime_string))
            h_client.update(updated_at=datetime.strptime(h_client['updated_at'], datetime_string))

            for idx, f_client in enumerate(forecast_clients_list):
                if h_client['harvest_id'] == f_client['harvest_id']:
                    h_client.update(forecast_id=f_client['id'])
                    forecast_clients_list.pop(idx)
                else:
                    pass

        # For each Client record, check if in db and update/insert accordingly
        print('Writing Clients')
        logger.print_progress_bar(iteration=0, total=len(harvest_client_list))
        for idx, client in enumerate(harvest_client_list):
            harvest_id = client['harvest_id']

            # If a Client is in db update, otherwise insert
            c = Client.get(harvest_id=harvest_id)
            if c:
                c.set(**client)
            else:
                nc = Client(harvest_id=harvest_id,
                            forecast_id=client['forecast_id'],
                            name=client['name'],
                            is_active=client['is_active'],
                            created_at=client['created_at'],
                            updated_at=client['updated_at'])

            # Commit the record
            db.commit()
            logger.print_progress_bar(iteration=idx + 1, total=len(harvest_client_list))

        # Cycle through remaining Forecast clients to update forecast_id, if needed
        for f_client in forecast_clients_list:
            is_active = not f_client.pop('archived')
            f_client.update(is_active=is_active)
            f_client.update(forecast_id=f_client.pop('id'))
            f_client.update(updated_at=datetime.strptime(f_client['updated_at'], datetime_str_ms))

            if f_client['harvest_id']:
                c = Client.get(harvest_id=f_client['harvest_id'])
                c.forecast_id = f_client['forecast_id']
            else:
                fc =Client.get(forecast_id=f_client['forecast_id'])
                # Update or insert the orphan Forecast client
                if fc:
                    fc.set(**f_client)
                else:
                    nfc = Client(forecast_id=f_client['forecast_id'],
                            name=f_client['name'],
                            is_active=f_client['is_active'],
                            updated_at=f_client['updated_at'])

    @db_session
    def munge_task(self):
        """Get all Harvest Tasks and send them to the db.

        :return:
        None
        """
        updated_since = self.task_last_updated
        # Get the Harvest Tasks List from its API
        harvest_tasks = self.harv.get_harvest_tasks(updated_since=updated_since)
        harvest_tasks_list = harvest_tasks['tasks']

        print('Writing Tasks')
        logger.print_progress_bar(iteration=0, total=len(harvest_tasks_list))
        for idx, task in enumerate(harvest_tasks_list):
            t_id = task['id']
            dt_updated_at = datetime.strptime(task['updated_at'], datetime_string)
            task.update(updated_at=dt_updated_at)

            # If a task is already in the DB, update it.  Otherwise insert it.
            if Task.get(id=t_id):
                Task[t_id].set(**task)
            else:
                t = Task(id=task['id'], name=task['name'], updated_at=task['updated_at'])

            # Commit the record to the db
            db.commit()
            logger.print_progress_bar(iteration=idx + 1, total=len(harvest_tasks_list))

    @db_session
    def munge_project(self):
        """Pulls Harvest and Forecast projects and inserts/updates records

        """
        updated_since = self.project_last_updated
        harvest_projects = self.harv.get_harvest_projects(updated_since=updated_since)
        harvest_projects_list = harvest_projects['projects']
        forecast_projects = self.fore.get_forecast_projects()
        forecast_projects_list = forecast_projects['projects']
        # Trim Forecast list based on updated_since var
        forecast_projects_list = self.__trim_forecast_results__(f_result_set=forecast_projects_list,
                                                               trim_datetime=updated_since)

        # Update Primary Key, get Forecast ID for each record
        for h_proj in harvest_projects_list:
            h_proj.update(harvest_id=h_proj.pop('id'))
            h_proj.update(forecast_id=None)
            # Convert the date keys into Python date objects so ORM can use them
            h_proj.update(created_at=datetime.strptime(h_proj['created_at'], datetime_string))
            h_proj.update(updated_at=datetime.strptime(h_proj['updated_at'], datetime_string))
            if h_proj['starts_on']:
                h_proj.update(starts_on=datetime.strptime(h_proj['starts_on'], date_string))
            if h_proj['ends_on']:
                h_proj.update(ends_on=datetime.strptime(h_proj['ends_on'], date_string))

            # Get Data Warehouse id for Client
            dw_client = get_client_by_id(identifier=h_proj['client_id'])
            h_proj.update(client_id=dw_client.id)

            # Get Forecast id
            for idx, f_proj in enumerate(forecast_projects_list):
                if h_proj['harvest_id'] == f_proj['harvest_id']:
                    h_proj.update(forecast_id=f_proj['id'])
                    forecast_projects_list.pop(idx)
                else:
                    pass

        # For each Project record, check if in db and update/insert accordingly
        print('Writing Projects')
        logger.print_progress_bar(iteration=0, total=len(harvest_projects_list))
        for idx, proj in enumerate(harvest_projects_list):
            try:
                harvest_id = proj['harvest_id']
            except Exception as e:
                pass

            # If a Project is in db update, otherwise insert
            pr = Project.get(harvest_id=harvest_id)
            if pr:
                pr.set(**proj)
            else:
                npr = Project(harvest_id=proj['harvest_id'],
                              forecast_id=proj['forecast_id'],
                              name=proj['name'],
                              code=proj['code'],
                              client_id=proj['client_id'],
                              client_name=proj['client_name'],
                              is_active=proj['is_active'],
                              is_billable=proj['is_billable'],
                              budget=proj['budget'],
                              budget_is_monthly=proj['budget_is_monthly'],
                              created_at=proj['created_at'],
                              updated_at=proj['updated_at'],
                              starts_on=proj['starts_on'],
                              ends_on=proj['ends_on'],)
            db.commit()
            logger.print_progress_bar(iteration=idx + 1, total=len(harvest_projects_list))

        # Cycle through remaining Forecast Projects to update records
        for f_proj in forecast_projects_list:
            f_proj.update(forecast_id=f_proj.pop('id'))
            f_proj.update(updated_at=datetime.strptime(f_proj['updated_at'], datetime_str_ms))
            if f_proj['starts_on']:
                f_proj.update(starts_on=datetime.strptime(f_proj['starts_on'], date_string))
            if f_proj['ends_on']:
                f_proj.update(ends_on=datetime.strptime(f_proj['ends_on'], date_string))

            if f_proj['harvest_id']:
                pr = Project.get(harvest_id=f_proj['harvest_id'])
                pr.forecast_id = f_proj['forecast_id']
            else:
                # Get Data Warehouse id for Client
                is_active = not f_proj.pop('archived')
                dw_client = get_client_by_id(f_proj['client_id'])
                # If no result is returned, set client to RevUnit
                if dw_client:
                    f_proj.update(client_id=dw_client.id)
                else:
                    f_proj.update(client_id=164)
                f_proj.update(client_name=dw_client.name)
                f_proj.update(is_active=is_active)

                # Update or insert the orphan Forecast client
                fpr = Project.get(forecast_id=f_proj['forecast_id'])
                if fpr:
                    fpr.set(**f_proj)
                else:
                    nfpr = Project(forecast_id=f_proj['forecast_id'],
                                   name=f_proj['name'],
                                   code=f_proj['code'],
                                   client_id=f_proj['client_id'],
                                   client_name=f_proj['client_name'],
                                   is_active=f_proj['is_active'],
                                   updated_at=f_proj['updated_at'],
                                   starts_on=f_proj['start_date'],
                                   ends_on=f_proj['end_date'],)
            db.commit()

    @db_session
    def munge_assignment(self):
        """Converts Forecast API into data warehouse friendly data

        Forecast API is very different than Harvest, so requires quite a bit of munging.
        Takes the date range from the Forecast entry and splits it into individual business day entries
        Calculates the hours/day for each assignment (source shows in seconds)
        Replaces API identity values with data warehouse ones
        """
        assignments = self.fore.get_forecast_assignments()
        assignments_list = assignments['assignments']

        # Get stats about the write
        total_parent_assns = len(assignments_list)

        print("Writing Assignments ({} Parent Assignments)".format(total_parent_assns))
        logger.print_progress_bar(iteration=0, total=total_parent_assns)
        for idx, assn in enumerate(assignments_list):
            # Grab information for split entries
            id = assn.pop('id')
            start_date = assn.pop('start_date')
            end_date = assn.pop('end_date')
            updated_at = assn.pop('updated_at')
            updated_at = datetime.strptime(updated_at, datetime_str_ms)

            # Convert Allocation to hours from seconds
            allocation = assn.pop('allocation')/3600

            # Update Assignment Project and Person fk's to match Data Warehouse
            pr = get_project_by_id(assn['project_id'])
            assn.update(project_id=pr.id)

            # Check if record has person id, if it does prepare data for next step
            if assn['person_id']:
                p = get_person_by_id(assn['person_id'])
                assn.update(person_id=p.id)
                # Generate date list between start/end of assignment
                dates = self.__make_date_list__(start=start_date, end=end_date)
            else:
                # Make dates an empty list so it does not write the split assignments to the db
                dates = []

            # Check if assignment records exist already with our parent id, delete if so
            a_recs = get_assignments_by_parent(parent_id=id)
            for rec in a_recs:
                Time_Assignment[rec.id].delete()
            db.commit()

            # For each business day in the assignment, make a new split entry
            for day in dates:
                if is_busday(day):
                    split_assn = assn.copy()
                    split_assn.update(parent_id=id)
                    split_assn.update(assign_date=(day).strftime(date_string))
                    split_assn.update(allocation=allocation)

                    # Insert the Time Assignment record
                    ta = Time_Assignment(parent_id=id,
                                         person_id=assn['person_id'],
                                         project_id=assn['project_id'],
                                         assign_date=day,
                                         allocation=allocation,
                                         updated_at=updated_at)
                else:
                    pass

            db.commit()
            logger.print_progress_bar(iteration=idx, total=total_parent_assns)

    """
    Utility Methods
    
    These help keep code less repetitive
    """

    def set_load_dates(self, is_full_load):
        """These dates determine the payload size of the calls made to Harvest and Forecast APIs

        When the "full_load" param is True, a date from 2010 is passed to ensure all records are returned, with the
        exception of Time Entries, which will use the system variable FROM_DATE since the payload from that is so large
        Otherwise a set of dates determined by the max updated_at field in the data warehouse are returned for each
        table.

        ToDos:
            -Split this into methods that returns results for individual tables rather than all tables at once to avoid
             errors if there are no records in a table.
        """
        if is_full_load:
            self.person_last_updated = self.full_load_datetime
            self.project_last_updated = self.full_load_datetime
            self.client_last_updated = self.full_load_datetime
            self.task_last_updated = self.full_load_datetime
            self.time_entry_last_updated = conf['FROM_DATE']
        else:
            self.person_last_updated = self.last_updated_dict['person'].strftime(datetime_string)
            self.project_last_updated = self.last_updated_dict['project'].strftime(datetime_string)
            self.client_last_updated = self.last_updated_dict['client'].strftime(datetime_string)
            self.task_last_updated = self.last_updated_dict['task'].strftime(datetime_string)
            self.time_entry_last_updated = self.last_updated_dict['time_entry'].strftime(datetime_string)

    def __trim_forecast_results__(self, f_result_set, trim_datetime):
        """This will trip the result set down to the updated_since date

        :returns list with dictionary entries that have an updated_at date greater than trim_datetime
        """
        for idx, f_obj in enumerate(f_result_set):
            if f_obj['updated_at'] <= trim_datetime:
                f_result_set.pop(idx)
            else:
                pass
        return f_result_set

    def __make_date_list__(self, start, end):
        # This takes two dates and provides the dates between them, inclusive of start and end
        start_date = datetime.strptime(start, date_string)
        end_date = datetime.strptime(end, date_string)
        dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

        return dates


class Munger(object):
    """This class will transform the Harvest and Forecast Data into records for the Data Warehouse

    It is being phased out by UberMunger, which will increase clarity in code and allow for improved differential load
    support.
    """

    def __init__(self, is_test=False):
        self.harv = Harvester(is_test=is_test)
        self.fore = Forecaster(is_test=is_test)
        self.mem_db = MemDB()
        self.last_updated_dict = get_updated_from_dates()
        self.full_load_datetime = '2010-01-01T00:00:00Z'

    """
    Munge Methods
    
    The methods below modify their namesake data in ways needed to be useful for the data warehouse tool.
    """

    def munge_person_list(self):
        """This will pull the Harvest and Forecast user/people lists and combine them into one entry

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
                o.update(full_name=str(o['first_name'] + ' ' + o['last_name']))
                harvest_people['people'].append(o)

        return harvest_people

    def munge_client_list(self):
        """Pulls the client list from Harvest and modifies the primary key for data warehouse

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
        """Performs data transformation needs for Projects in the Data Warehouse

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
                    dw_client = get_client_by_id(o['client_id'])
                    o.update(client_id=dw_client.id)
                    o.update(client_name=dw_client.name)
                else: # It's the Time Off Project, assign to RevUnit as Client ID and Name
                    o.update(client_id=164)
                    o.update(client_name='RevUnit')

                harvest_projects['projects'].append(o)

        return harvest_projects

    def munge_task_list(self):
        """Grabs tasks list from Harvest and returns them.  No transoformation needed.

        """

        tasks = self.harv.get_harvest_tasks(updated_since=self.task_last_updated)
        return tasks

    def munge_harvest_time_entries(self):
        """Transforms the time entries table into data warehouse data.

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
        """This allows for filling legacy Entries table while reports are migrated to new format

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
        """Converts Forecast API into data warehouse friendly data

        The Forecast API is very different than Harvest, so it requires quite a bit of munging.
        Takes the date range from the Forecast entry and splits it into individual business day entries
        Calculates the hours/day for each assignment (source shows in seconds)
        Replaces API identity values with data warehouse ones
        """

        assignments = self.fore.get_forecast_assignments()
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

    """
    Utility Methods 
    
    These help keep code less reptitive
    """

    def set_load_dates(self, is_full_load):
        """These dates determine the payload size of the calls made to Harvest and Forecast APIs

        When the "full_load" param is True, a date from 2010 is passed to ensure all records are returned, with the
        exception of Time Entries, which will use the system variable FROM_DATE since the payload from that is so large

        Otherwise a set of dates determined by the max updated_at field in the data warehouse are returned for each
        table.

        ToDos
            Split this into a method that returns results for individual tables rather than all tables at once to avoid
            errors if there are no records in a table.
        """
        if is_full_load:
            self.person_last_updated = self.full_load_datetime
            self.project_last_updated = self.full_load_datetime
            self.client_last_updated = self.full_load_datetime
            self.task_last_updated = self.full_load_datetime
            self.time_entry_last_updated = conf['FROM_DATE']
        else:
            self.person_last_updated = self.last_updated_dict['person'].strftime(datetime_string)
            self.project_last_updated = self.last_updated_dict['project'].strftime(datetime_string)
            self.client_last_updated = self.last_updated_dict['client'].strftime(datetime_string)
            self.task_last_updated = self.last_updated_dict['task'].strftime(datetime_string)
            self.time_entry_last_updated = self.last_updated_dict['time_entry'].strftime(datetime_string)

    def __insert_harvest_id__(self, result_dict):
        # Insert Harvest ID, pop the old id key (Will be replaced by db identity key upon insert)

        if 'id' in result_dict:
            result_dict.update(harvest_id=result_dict.pop('id'))

        else:
            print('No id column found')

        return result_dict

    def __make_date_list__(self, start, end):
        # This takes two dates and provides the dates between them, inclusive of start and end
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
        """Takes a single harvest object (Like a Project or Person dict) and checks if a corresponding value exists in a
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
