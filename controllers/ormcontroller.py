"""
All Classes needed for ORM management of DB
"""

from datetime import date
from datetime import datetime
from decimal import Decimal
from urllib.parse import urlparse
from data_rocket_conf import config as conf
from pony.orm import *

# Define the required Database object for PonyORM
db = Database()

# Create User ORM Object
class User(db.Entity):
    id = PrimaryKey(int, auto=False)
    first_name = Optional(str)
    last_name = Optional(str)
    forecast_id = Optional(int)
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
    user_id = Required(User)
    user_name = Optional(str)
    client_id = Required('Client')
    client_name = Optional(str)
    project_id = Required('Project')
    project_name = Optional(str)
    task_id = Required('Task')
    task_name = Optional(str)

# Create Project ORM Object
class Project(db.Entity):
    id = PrimaryKey(int, auto=False)
    time_entries = Set(TimeEntry)
    name = Optional(str)
    forecast_id = Optional(int)

# Create Client ORM Object
class Client(db.Entity):
    id = PrimaryKey(int, auto=False)
    name = Optional(str)
    forecast_id = Optional(int)
    time_entries = Set(TimeEntry)

# Create Task ORM Object
class Task(db.Entity):
    id = PrimaryKey(int, auto=False)
    name = Optional(str)
    time_entries = Set(TimeEntry)

# Parse URL to get connection info
pg_url = urlparse(conf['DB_CONN'])

# Create database binding to PostgreSQL
db.bind(provider='postgres', user=pg_url.username, database=pg_url.path[1:],
        host=pg_url.hostname, password=pg_url.password, port=pg_url.port)

set_sql_debug(False)
# Create ORM mapping, tables if necessary
db.generate_mapping(create_tables=True)

def insert_time_entry_list(time_entry_list):
    with db_session:
        for entry in time_entry_list:
            e = db.insert('timeentry',id=entry['id'], spent_date=entry['spent_date'],hours=entry['hours'],
                          billable=entry['billable'], billable_rate=entry['billable_rate'],
                          created_at=entry['created_at'], updated_at=entry['updated_at'],
                          entry_amount=entry['entry_amount'], user_id=entry['user_id'],
                          user_name=entry['user_name'],client_id=entry['client_id'],
                          client_name=entry['client_name'],project_id=entry['project_id'],
                          project_name=entry['project_name'], task_id=entry['task_id'], task_name=entry['task_name'])