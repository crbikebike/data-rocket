"""
All Classes needed for ORM management of DB
Replaces the dbcontroller.py file from previous versions
"""

from datetime import date
from datetime import datetime
from decimal import Decimal
from urllib.parse import urlparse
from data_rocket_conf import config as conf
from pony.orm import *
from psycopg2.extensions import AsIs

# Define the required Database object for PonyORM
db = Database()

"""
All the classes below map directly to the tables and their fields
"""

class Person(db.Entity):
    id = PrimaryKey(int, auto=True)
    harvest_id = Optional(int, unique=True)
    forecast_id = Optional(int, unique=True, nullable=True)
    first_name = Optional(str)
    last_name = Optional(str)
    weekly_goal = Optional(Decimal)
    yearly_goal = Optional(int)
    time_entries = Set('Time_Entry')
    email = Optional(str)
    timezone = Optional(str)
    weekly_capacity = Optional(Decimal)
    is_contractor = Optional(bool)
    is_active = Optional(bool)
    roles = Optional(Json)
    avatar_url = Optional(str)
    created_at = Optional(datetime)
    updated_at = Optional(datetime)
    assignments = Set('Time_Assignment')


class Time_Entry(db.Entity):
    id = PrimaryKey(int, auto=True)
    spent_date = Optional(date)
    hours = Optional(Decimal)
    billable = Optional(bool)
    billable_rate = Optional(Decimal)
    created_at = Optional(datetime)
    updated_at = Optional(datetime)
    entry_amount = Optional(Decimal)
    person_id = Required(Person)
    person_name = Optional(str)
    project_id = Required('Project')
    project_name = Optional(str)
    project_code = Optional(str)
    client_id = Required('Client')
    client_name = Optional(str)
    task_id = Required('Task')
    task_name = Optional(str)


class Project(db.Entity):
    id = PrimaryKey(int, auto=True)
    harvest_id = Optional(int, unique=True)
    forecast_id = Optional(int, unique=True, nullable=True)
    time_entries = Set(Time_Entry)
    name = Optional(str)
    code = Optional(float)
    client_id = Required('Client')
    client_name = Optional(str)
    is_active = Optional(bool)
    is_billable = Optional(bool)
    budget = Optional(Decimal)
    budget_is_monthly = Optional(bool)
    created_at = Optional(datetime)
    updated_at = Optional(datetime)
    starts_on = Optional(date)
    ends_on = Optional(date)
    assignments = Set('Time_Assignment')


class Client(db.Entity):
    id = PrimaryKey(int, auto=True)
    harvest_id = Optional(int, unique=True)
    name = Optional(str)
    is_active = Optional(str)
    time_entries = Set(Time_Entry)
    projects = Set(Project)
    created_at = Optional(datetime)
    updated_at = Optional(datetime)


class Task(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Optional(str)
    time_entries = Set(Time_Entry)


class Time_Assignment(db.Entity):
    id = PrimaryKey(int, auto=True)
    parent_id = Optional(int)
    person_id = Required(Person)
    assign_date = Optional(date)
    allocation = Optional(Decimal)
    updated_at = Optional(datetime)
    project_id = Required(Project)


# Create log table
class DataRocketLog(db.Entity):
    id = PrimaryKey(int, auto=True)
    event_description =  Required(str)
    event_datetime = Required(datetime)
    event_success = Required(bool)


# Parse URL to get connection info
pg_url = urlparse(conf['DB_CONN'])

# Create database binding to PostgreSQL
db.bind(provider='postgres', user=pg_url.username, database=pg_url.path[1:],
        host=pg_url.hostname, password=pg_url.password, port=pg_url.port)

set_sql_debug(False)
# Create ORM mapping, tables if necessary
db.generate_mapping(create_tables=True)


"""
CREATE:
The below insert functions each insert data for their namesake.  They assume being passed a list of dictionaries with
column names and values that correspond to the ORM classes above.
"""


@db_session
def insert_time_entries_list(time_entry_list):
    # Loop through every entry in the list and write to db
    record_count = len(time_entry_list)
    error_count = 0
    for entry in time_entry_list:
        columns = AsIs(','.join(entry.keys()))
        values = tuple([entry[column] for column in entry.keys()])
        try:
            db.execute("INSERT INTO public.time_entry ($columns) VALUES $values")
        except Exception as e:
            error_count +=1
    p = calc_error_percent(record_count, error_count)
    print("Errors while inserting time_entries: {err} ({p})".format(err=error_count, p=p))


@db_session
def insert_tasks_list(tasks_list):
    # Loop through every task in the list and write to db
    record_count = len(tasks_list)
    error_count = 0
    for task in tasks_list:
        columns = AsIs(','.join(task.keys()))
        values = tuple([task[column] for column in task.keys()])
        try:
            db.execute("INSERT INTO public.task ($columns) VALUES $values")
        except Exception as e:
            error_count +=1
    p = calc_error_percent(record_count, error_count)
    print("Errors while inserting tasks: {err} ({p})".format(err=error_count, p=p))


@db_session
def insert_clients_list(client_list):
    # Loop through every client in the list and write to db
    record_count = len(client_list)
    error_count = 0
    for client in client_list:
        columns = AsIs(','.join(client.keys()))
        values = tuple([client[column] for column in client.keys()])
        try:
            db.execute("INSERT INTO public.client ($columns) VALUES $values")
        except Exception as e:
            error_count +=1
    p = calc_error_percent(record_count, error_count)
    print("Errors while inserting clients: {err} ({p})".format(err=error_count, p=p))


@db_session
def insert_projects_list(project_list):
    # Loop through every project in the list and write to db
    record_count = len(project_list)
    error_count = 0
    for project in project_list:
        columns = AsIs(','.join(project.keys()))
        values = tuple([project[column] for column in project.keys()])
        try:
            db.execute("INSERT INTO public.project ($columns) VALUES $values")
        except Exception as e:
            error_count +=1
    p = calc_error_percent(record_count, error_count)
    print("Errors while inserting projects: {err} ({p})".format(err=error_count, p=p))


@db_session
def insert_people_list(people_list):
    # Loop through every person in the list and write to db
    record_count = len(people_list)
    error_count = 0
    for person in people_list:
        columns = AsIs(','.join(person.keys()))
        values = tuple([person[column] for column in person.keys()])
        try:
            db.execute("INSERT INTO public.person ($columns) VALUES $values")
        except Exception as e:
            error_count +=1
    p = calc_error_percent(record_count, error_count)
    print("Errors while inserting people: {err} ({p})".format(err=error_count, p=p))


@db_session
def insert_time_assignment_list(assignment_list):
    # Loop through every time assignment in the list and write to db
    record_count = len(assignment_list)
    error_count = 0
    for assn in assignment_list:
        columns = AsIs(','.join(assn.keys()))
        values = tuple([assn[column] for column in assn.keys()])
        try:
            db.execute("INSERT INTO public.time_assignment ($columns) VALUES $values")
        except Exception as e:
            error_count +=1
    p = calc_error_percent(record_count, error_count)
    print("Errors while inserting assignments: {err} ({p})".format(err=error_count, p=p))


"""
READ:
The following fuctions fetch info from the db
"""


@db_session
def get_person_table():
    ppl_tbl = select(person for person in Person)[:]
    return ppl_tbl


@db_session
def get_person_by_harvest_id(harvest_id):
    person = Person.get(harvest_id=harvest_id)
    return person


@db_session
def get_person_by_forecast_id(forecast_id):
    person = Person.get(harvest_id=forecast_id)
    return person


@db_session
def get_project_table():
    proj_tbl = select(proj for proj in Project)[:]
    return proj_tbl


@db_session
def get_project_by_harvest_id(harvest_id):
    project = Project.get(harvest_id=harvest_id)
    return project


@db_session
def get_project_by_forecast_id(forecast_id):
    project = Project.get(forecast_id=forecast_id)
    return project


@db_session
def get_client_table():
    client_tbl = select(clnt for clnt in Client)[:]
    return client_tbl


@db_session
def get_client_by_harvest_id(harvest_id):
    client = Client.get(harvest_id=harvest_id)
    return client


"""
Utility Functions
These keep code above as clean and non-repetitive as possible.
"""


def calc_error_percent(record_count, error_count):
    percent = (error_count / record_count) * 100
    pretty_percent = str(percent) + '%'

    return pretty_percent