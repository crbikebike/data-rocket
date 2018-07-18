"""
This file takes all API data, combines, transforms, and calculates it before it's ready to be sent off to the DB
It will greatly simplify the actions taken within the main.py file
"""

# Imports
from controllers.datagrabber import Harvester, Forecaster
from controllers.ormcontroller import *
from controllers.utilitybot import datetime_format, date_format, datetime_format_ms, logger, full_load_datetime
from datetime import datetime, timedelta
from numpy import is_busday
from data_rocket_conf import config as conf

# Classes
class UberMunge(object):
    """This class transforms data before sending off to the DB for insert or update

    The general principle of UberMunge is to push data to the db immediately after transformation as an insert or
    update rather than load in a bulk transaction after all records are transformed.
    """
    def __init__(self, is_test=False):
        self.harv = Harvester(is_test=is_test)
        self.fore = Forecaster(is_test=is_test)
        self.last_updated_dict = get_updated_from_dates()

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
            h_person.update(created_at=datetime.strptime(h_person['created_at'], datetime_format))
            h_person.update(updated_at=datetime.strptime(h_person['updated_at'], datetime_format))

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
            try:
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
            except Exception as e:
                desc = "Person Entry Error - id: {}".format(person['harvest_id'])
                logger.write_load_completion(documents=str(e), description=desc)
            # Update the on-screen progress bar
            logger.print_progress_bar(iteration=idx + 1, total=len(harvest_people_list))

        # Cycle through remaining Forecast people to update forecast_id, if needed
        for f_person in forecast_people_list:
            f_person.update(forecast_id=f_person.pop('id'))
            full_name = "{fn} {ln}".format(fn=f_person['first_name'], ln=f_person['last_name'])
            is_active = not f_person.pop('archived')
            f_person.update(is_active=is_active)
            f_person.update(updated_at=datetime.strptime(f_person['updated_at'], datetime_format_ms))

            if f_person['harvest_id']:
                p = Person.get(harvest_id=f_person['harvest_id'])
                p.forecast_id = f_person['forecast_id']
            else:
                # If orphan Person exists, update. Else, insert.
                try:
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
                except Exception as e:
                    desc = "Forecast Person Entry Error - id: {}".format(f_person['forecast_id'])
                    logger.write_load_completion(documents=str(e), description=desc)
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
            h_client.update(created_at=datetime.strptime(h_client['created_at'], datetime_format))
            h_client.update(updated_at=datetime.strptime(h_client['updated_at'], datetime_format))

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
            try:
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
            except Exception as e:
                desc = "Client Entry Error - id: {}".format(client['harvest_id'])
                logger.write_load_completion(documents=str(e), description=desc)

            # Update the on-screen progress bar
            logger.print_progress_bar(iteration=idx + 1, total=len(harvest_client_list))

        # Cycle through remaining Forecast clients to update forecast_id, if needed
        for f_client in forecast_clients_list:
            is_active = not f_client.pop('archived')
            f_client.update(is_active=is_active)
            f_client.update(forecast_id=f_client.pop('id'))
            f_client.update(updated_at=datetime.strptime(f_client['updated_at'], datetime_format_ms))

            if f_client['harvest_id']:
                c = Client.get(harvest_id=f_client['harvest_id'])
                c.forecast_id = f_client['forecast_id']
            else:
                try:
                    fc =Client.get(forecast_id=f_client['forecast_id'])
                    # Update or insert the orphan Forecast client
                    if fc:
                        fc.set(**f_client)
                    else:
                        nfc = Client(forecast_id=f_client['forecast_id'],
                                name=f_client['name'],
                                is_active=f_client['is_active'],
                                updated_at=f_client['updated_at'])
                except:
                    desc = "Forecast Client Entry Error - id: {}".format(f_client['forecast_id'])
                    logger.write_load_completion(documents=str(e), description=desc)

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
            dt_updated_at = datetime.strptime(task['updated_at'], datetime_format)
            task.update(updated_at=dt_updated_at)

            # If a task is already in the DB, update it.  Otherwise insert it.
            try:
                if Task.get(id=t_id):
                    Task[t_id].set(**task)
                else:
                    t = Task(id=task['id'], name=task['name'], updated_at=task['updated_at'])
                # Commit the record to the db
                db.commit()
            except Exception as e:
                desc = "Task Entry Error - id: {}".format(task['id'])
                logger.write_load_completion(documents=str(e), description=desc)
            # Update the on-screen progress bar
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
            h_proj.update(created_at=datetime.strptime(h_proj['created_at'], datetime_format))
            h_proj.update(updated_at=datetime.strptime(h_proj['updated_at'], datetime_format))
            if h_proj['starts_on']:
                h_proj.update(starts_on=datetime.strptime(h_proj['starts_on'], date_format))
            if h_proj['ends_on']:
                h_proj.update(ends_on=datetime.strptime(h_proj['ends_on'], date_format))

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
            harvest_id = proj['harvest_id']

            # If a Project is in db update, otherwise insert
            try:
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
            except Exception as e:
                desc = "Project Entry Error - id: {}".format(proj['harvest_id'])
                logger.write_load_completion(documents=str(e), description=desc)
            #Update on-screen progress bar
            logger.print_progress_bar(iteration=idx + 1, total=len(harvest_projects_list))

        # Cycle through remaining Forecast Projects to update records
        for f_proj in forecast_projects_list:
            f_proj.update(forecast_id=f_proj.pop('id'))
            f_proj.update(updated_at=datetime.strptime(f_proj['updated_at'], datetime_format_ms))
            if f_proj['starts_on']:
                f_proj.update(starts_on=datetime.strptime(f_proj['starts_on'], date_format))
            if f_proj['ends_on']:
                f_proj.update(ends_on=datetime.strptime(f_proj['ends_on'], date_format))

            # If it has a harvest id, just update the forecast id of the data warehouse project
            if f_proj['harvest_id']:
                pr = Project.get(harvest_id=f_proj['harvest_id'])
                pr.forecast_id = f_proj['forecast_id']
            else:
                # If it doesn't have a harvest id, transform data and insert/update data warehouse
                is_active = not f_proj.pop('archived')
                f_proj.update(is_active=is_active)

                # Check for a client ID. If no result is returned, set client to RevUnit
                if f_proj['client_id']:
                    dw_client = get_client_by_id(f_proj['client_id'])
                    f_proj.update(client_id=dw_client.id)
                    f_proj.update(client_name=dw_client.name)
                else:
                    f_proj.update(client_id=164)
                    f_proj.update(client_name='RevUnit')

                # Update or insert the orphan Forecast client
                try:
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
                                       starts_on=f_proj['starts_on'],
                                       ends_on=f_proj['ends_on'],)
                except Exception as e:
                    desc = "Forecast Project Entry Error - id: {}".format(f_proj['forecast_id'])
                    logger.write_load_completion(documents=str(e), description=desc)
            db.commit()

    @db_session
    def munge_time_entries(self):
        """Pulls Time Entries for a given range and sends them to the data warehouse

        """
        last_updated = self.time_entry_last_updated
        entries = self.harv.get_harvest_time_entries(updated_since=last_updated)
        entries_list = entries['time_entries']
        # Get stats for console progress bar
        total_entries = len(entries_list)

        print("Writing Time Entries ({} total)".format(total_entries))
        logger.print_progress_bar(iteration=0, total=total_entries)
        for idx, entry in enumerate(entries_list):
            # Convert dates to ORM friendly Python objects
            entry.update(spent_date=datetime.strptime(entry['spent_date'], date_format))
            entry.update(created_at=datetime.strptime(entry['created_at'], datetime_format))
            entry.update(updated_at=datetime.strptime(entry['updated_at'], datetime_format))

            # Make keys data warehouse friendly
            entry.update(person_id=entry.pop('user_id'))
            entry.update(person_name=entry.pop('user_name'))

            # Calculate the total entry value
            if entry['billable_rate']:
                entry.update(entry_amount=(entry['hours'] * entry['billable_rate']))
            else:
                entry.update(entry_amount=0)

            # Update person, project, and client fk's to match data warehouse
            p = get_person_by_id(entry['person_id'])
            entry.update(person_id=p.id)
            pr = get_project_by_id(entry['project_id'])
            entry.update(project_id=pr.id)
            c = get_client_by_id(entry['client_id'])
            entry.update(client_id=c.id)

            try:
                # If entry exists, update.  Else write new entry
                te = Time_Entry.get(id=entry['id'])
                if te:
                    te.set(**entry)
                else:
                # Write the new time entry
                    nte = Time_Entry(id=entry['id'],
                                    spent_date=entry['spent_date'],
                                    hours=entry['hours'],
                                    billable=entry['billable'],
                                    billable_rate=entry['billable_rate'],
                                    created_at=entry['created_at'],
                                    updated_at=entry['updated_at'],
                                    entry_amount=entry['entry_amount'],
                                    person_id=entry['person_id'],
                                    person_name=entry['person_name'],
                                    project_id=entry['project_id'],
                                    project_name=entry['project_name'],
                                    project_code=entry['project_code'],
                                    client_id=entry['client_id'],
                                    client_name=entry['client_name'],
                                    task_id=entry['task_id'],
                                    task_name=entry['task_name'])
            except Exception as e:
                desc = "Time Entry Error - id: {}".format(entry['id'])
                logger.write_load_completion(documents=str(e), description=desc)

            # Commit entries
            db.commit()
            logger.print_progress_bar(iteration=idx + 1, total=total_entries)

        # Trunc legacy entries table and copy time_entry values to it
        print("Copying records to legacy entries table")
        trunc_legacy_entries()
        copy_to_legacy_entries()

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

        # Trim Assignments list by updated date
        updated_since = self.assn_last_updated
        assignments_list = self.__trim_forecast_results__(f_result_set=assignments_list, trim_datetime=updated_since)

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
            updated_at = datetime.strptime(updated_at, datetime_format_ms)

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
                    split_assn.update(assign_date=(day).strftime(date_format))
                    split_assn.update(allocation=allocation)

                    # Insert the Time Assignment record
                    try:
                        ta = Time_Assignment(parent_id=id,
                                             person_id=assn['person_id'],
                                             project_id=assn['project_id'],
                                             assign_date=day,
                                             allocation=allocation,
                                             updated_at=updated_at)
                        db.commit()
                    except Exception as e:
                        desc = "Time Assignment Error - id: {}".format(assn['id'])
                        logger.write_load_completion(documents=str(e), description=desc)
                else:
                    pass

            # Update the on-screen progress bar
            logger.print_progress_bar(iteration=idx + 1, total=total_parent_assns)

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
            self.person_last_updated = full_load_datetime
            self.project_last_updated = full_load_datetime
            self.client_last_updated = full_load_datetime
            self.task_last_updated = full_load_datetime
            self.assn_last_updated = full_load_datetime
            self.time_entry_last_updated = conf['FROM_DATE']
        else:
            self.person_last_updated = self.last_updated_dict['person'].strftime(datetime_format)
            self.project_last_updated = self.last_updated_dict['project'].strftime(datetime_format)
            self.client_last_updated = self.last_updated_dict['client'].strftime(datetime_format)
            self.task_last_updated = self.last_updated_dict['task'].strftime(datetime_format)
            self.assn_last_updated = self.last_updated_dict['time_assignment'].strftime(datetime_format)
            self.time_entry_last_updated = self.last_updated_dict['time_entry'].strftime(datetime_format)

    def __trim_forecast_results__(self, f_result_set, trim_datetime):
        """This will trim the result set by filtering to items that have a greater updated_at date than trim date

        :returns list with dictionary entries that have an updated_at date greater than trim_datetime
        """
        filter_list = [f_obj for f_obj in f_result_set if f_obj['updated_at'] >= trim_datetime]

        return filter_list

    def __make_date_list__(self, start, end):
        # This takes two dates and provides the dates between them, inclusive of start and end
        start_date = datetime.strptime(start, date_format)
        end_date = datetime.strptime(end, date_format)
        dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

        return dates