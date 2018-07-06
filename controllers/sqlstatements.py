"""
This file is being deprecated in favor of the ORM solution in the ormcontroller.py file
"""

sqldict = {
    'create_entry_table': """CREATE TABLE IF NOT EXISTS harvest_entries
    (entry_id int PRIMARY KEY,
    user_id int NOT NULL,
    user_name varchar(256) NOT NULL,
    client_id int NOT NULL,
    client_name varchar(256),
    harvest_project_id int,
    harvest_project_name varchar(256),
    harvest_project_code varchar(256),
    task_name varchar(256),
    task_id int,
    billable_rate int,
    created_at timestamp,
    hours numeric(5,2),
    spent_date timestamp,
    updated_at timestamp,
    billable boolean,
    entry_amount numeric(10,2))
    """,
    'drop_entry_table': """DROP TABLE IF EXISTS harvest_entries""",
    'insert_entry_table': """INSERT INTO harvest_entries (%s) VALUES %s"""
}

sql_creates = [
    {'table1':'things'},
    {'table2': 'things'}
]

sql_reads = {}


sqldict_test = {
    'create_entry_table': """CREATE TABLE IF NOT EXISTS harvest_entries_test
    (entry_id int PRIMARY KEY,
    user_id int NOT NULL,
    user_name varchar(50) NOT NULL,
    client_id int NOT NULL,
    client_name varchar(50),
    harvest_project_id int,
    harvest_project_name varchar(50),
    harvest_project_code int,
    task_name varchar(50),
    task_id int,
    billable_rate int,
    created_at timestamp,
    hours numeric(5,2),
    spent_date timestamp,
    updated_at timestamp,
    billable boolean,
    entry_amount numeric(10,2))
    """,
    'drop_table': """DROP TABLE IF EXISTS harvest_entries_test""",
    'insert_entry': """INSERT INTO harvest_entries_test (%s) VALUES(%s)"""
}

#Need to reformat for test Harvest entries
test_data = [
    {},

    {},

    {},

    {},

    {},

    {}
]