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

# Create User ORM Object
class Person(db.Entity):
    id = PrimaryKey(int, auto=False)
    first_name = Optional(str)
    last_name = Optional(str)
    # forecast_id = Optional(int)
    weekly_goal = Optional(Decimal)
    yearly_goal = Optional(int)
    time_entries = Set('TimeEntry')
    email = Optional(str)
    timezone = Optional(str)
    weekly_capacity = Optional(Decimal)
    is_contractor = Optional(bool)
    is_active = Optional(bool)
    roles = Optional(Json)
    avatar_url = Optional(str)
    created_at = Optional(datetime)
    updated_at = Optional(datetime)

# Create Harvest Time Entry ORM Object
class TimeEntry(db.Entity):
    id = PrimaryKey(int, auto=False)
    spent_date = Optional(date)
    hours = Optional(Decimal)
    billable = Optional(bool)
    billable_rate = Optional(Decimal)
    created_at = Optional(datetime)
    updated_at = Optional(datetime)
    entry_amount = Optional(Decimal)
    user_id = Required(Person)
    user_name = Optional(str)
    client_id = Required('Client')
    client_name = Optional(str)
    project_id = Required('Project')
    project_name = Optional(str)
    project_code = Optional(str)
    task_id = Required('Task')
    task_name = Optional(str)

# Create Project ORM Object
class Project(db.Entity):
    id = PrimaryKey(int, auto=False)
    time_entries = Set(TimeEntry)
    name = Optional(str)
    code = Optional(str)
    # forecast_id = Optional(int)
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


# Create Client ORM Object
class Client(db.Entity):
    id = PrimaryKey(int, auto=False)
    name = Optional(str)
    is_active = Optional(bool)
    # forecast_id = Optional(int)
    time_entries = Set(TimeEntry)
    projects = Set(Project)
    created_at = Optional(datetime)
    updated_at = Optional(datetime)

# Create Task ORM Object
class Task(db.Entity):
    id = PrimaryKey(int, auto=False)
    name = Optional(str)
    time_entries = Set(TimeEntry)

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
The below insert functions each insert data for their namesake.  They assume being passed a list of dictionaries with
column names and values that correspond to the ORM classes above.
"""

@db_session
def insert_time_entries_list(time_entry_list):
    # Loop through every entry in the list and write to db
    for entry in time_entry_list:
        columns = AsIs(','.join(entry.keys()))
        values = tuple([entry[column] for column in entry.keys()])
        db.execute("INSERT INTO public.timeentry ($columns) VALUES $values")

@db_session
def insert_tasks_list(tasks_list):
    # Loop through every task in the list and write to db
    for task in tasks_list:
        columns = AsIs(','.join(task.keys()))
        values = tuple([task[column] for column in task.keys()])
        db.execute("INSERT INTO public.task ($columns) VALUES $values")

@db_session
def insert_clients_list(client_list):
    # Loop through every client in the list and write to db
    for client in client_list:
        columns = AsIs(','.join(client.keys()))
        values = tuple([client[column] for column in client.keys()])
        db.execute("INSERT INTO public.client ($columns) VALUES $values")

@db_session
def insert_projects_list(project_list):
    # Loop through every project in the list and write to db
    for project in project_list:
        columns = AsIs(','.join(project.keys()))
        values = tuple([project[column] for column in project.keys()])
        db.execute("INSERT INTO public.project ($columns) VALUES $values")

@db_session
def insert_people_list(people_list):
    # Loop through every person in the list and write to db
    for person in people_list:
        columns = AsIs(','.join(person.keys()))
        values = tuple([person[column] for column in person.keys()])
        db.execute("INSERT INTO public.person ($columns) VALUES $values")