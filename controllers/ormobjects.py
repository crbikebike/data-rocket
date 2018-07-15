"""
All Classes needed for ORM management of DB
Import this file to the controller you want to connect to the db
"""

from datetime import date
from datetime import datetime
from decimal import Decimal
from pony.orm import *


# Define the required Database object for PonyORM
db = Database()

"""
All the classes below map directly to the tables and their fields
"""

class Person(db.Entity):
    id = PrimaryKey(int, auto=True)
    harvest_id = Optional(int, unique=True, nullable=True)
    forecast_id = Optional(int, unique=True, nullable=True)
    first_name = Optional(str)
    last_name = Optional(str)
    full_name = Optional(str, nullable=True)
    weekly_goal = Optional(Decimal, nullable=True)
    yearly_goal = Optional(int, nullable=True)
    time_entries = Set('Time_Entry')
    email = Optional(str, nullable=True)
    timezone = Optional(str, nullable=True)
    weekly_capacity = Optional(Decimal)
    is_contractor = Optional(bool)
    is_active = Optional(bool)
    roles = Optional(Json)
    avatar_url = Optional(str, nullable=True)
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
    harvest_id = Optional(int, unique=True, nullable=True)
    forecast_id = Optional(int, unique=True, nullable=True)
    time_entries = Set(Time_Entry)
    name = Optional(str)
    code = Optional(str, nullable=True)
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
    harvest_id = Optional(int, unique=True, nullable=True)
    forecast_id = Optional(int, unique=True, nullable=True)
    name = Optional(str)
    is_active = Optional(bool)
    time_entries = Set(Time_Entry)
    projects = Set(Project)
    created_at = Optional(datetime)
    updated_at = Optional(datetime)


class Task(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Optional(str)
    time_entries = Set(Time_Entry)
    updated_at = Optional(datetime)


class Time_Assignment(db.Entity):
    id = PrimaryKey(int, auto=True)
    parent_id = Optional(int)
    person_id = Required(Person)
    project_id = Required(Project)
    assign_date = Optional(date)
    allocation = Optional(Decimal)
    updated_at = Optional(datetime)



class Harvest_Entries(db.Entity):
    """Legacy Entry Table"""
    entry_id = PrimaryKey(int)
    hours = Optional(Decimal)
    spent_date = Optional(datetime)
    billable = Optional(bool)
    billable_rate = Optional(Decimal)
    created_at = Optional(datetime)
    updated_at = Optional(datetime)
    entry_amount = Optional(Decimal)
    user_id = Optional(int)
    user_name = Optional(str, nullable=True)
    harvest_project_id = Optional(int)
    harvest_project_name = Optional(str, nullable=True)
    harvest_project_code = Optional(str, nullable=True)
    client_id = Optional(int)
    client_name = Optional(str, nullable=True)
    task_id = Optional(int)
    task_name = Optional(str, nullable=True)


# Create log table
class DataRocketLog(db.Entity):
    id = PrimaryKey(int, auto=True)
    event_description =  Required(str)
    event_datetime = Required(datetime)
    event_success = Required(bool)
    event_documents = Optional(Json)
