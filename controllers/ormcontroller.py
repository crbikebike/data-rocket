"""
All Classes needed for ORM management of DB
"""

from datetime import date
from datetime import datetime
from decimal import Decimal
from pony.orm import *


db = Database()


class User(db.Entity):
    id = PrimaryKey(int, auto=True)
    first_name = Optional(str, 256)
    last_name = Optional(str, 256)
    forecast_id = Optional(int, size=24)
    weekly_goal = Optional(Decimal)
    yearly_goal = Optional(int, size=24)
    time_entries = Set('TimeEntry')
    email = Optional(str, 256)
    timezone = Optional(str, 256)
    weekly_capacity = Optional(Decimal)
    is_contractor = Optional(bool)
    is_active = Optional(bool)
    roles = Optional(Json, 512)
    avatar_url = Optional(str, 512)
    created_at = Optional(datetime)
    updated_at = Optional(datetime)


class TimeEntry(db.Entity):
    id = PrimaryKey(int, auto=True)
    spent_date = Optional(date)
    hours = Optional(Decimal)
    billable = Optional(bool)
    billable_rate = Optional(Decimal)
    created_at = Optional(datetime)
    updated_at = Optional(datetime)
    entry_amount = Optional(Decimal)
    user_id = Required(User)
    user_name = Optional(str, 256)
    client_id = Required('Client')
    client_name = Optional(str, 256)
    project_id = Required('Project')
    project_name = Optional(str)
    task_id = Required('Task')
    task_name = Optional(str, 256)


class Project(db.Entity):
    id = PrimaryKey(int, auto=True)
    time_entries = Set(TimeEntry)
    name = Optional(str, 256)
    forecast_id = Optional(int)


class Client(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Optional(str, 256)
    forecast_id = Optional(int)
    time_entries = Set(TimeEntry)


class Task(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Optional(str, 256)
    time_entries = Set(TimeEntry)



db.generate_mapping()