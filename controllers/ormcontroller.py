"""
All transactions from the objects store in the ormobjects file
Replaces the dbcontroller.py file from previous versions
"""

from urllib.parse import urlparse
from data_rocket_conf import config as conf
from psycopg2.extensions import AsIs
from controllers.ormobjects import *

if conf['DB_CONN']:
    # Parse URL to get connection info
    pg_url = urlparse(conf['DB_CONN'])

    # Create database binding to PostgreSQL
    db.bind(provider='postgres', user=pg_url.username, database=pg_url.path[1:],
            host=pg_url.hostname, password=pg_url.password, port=pg_url.port)
else:
    # If running unit test, use sqlite in memory
    db.bind(provider='sqlite', filename=':memory:')
    print("RUNNING UNIT TESTS ON DB IN MEMORY.  IF THAT IS NOT INTENDED CHECK YOUR CONF FILE")

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
    # logger.print_progress_bar(iteration=0, total=record_count)
    for idx, entry in enumerate(time_entry_list):
        columns = AsIs(','.join(entry.keys()))
        values = tuple([entry[column] for column in entry.keys()])
        try:
            db.execute("INSERT INTO public.time_entry ($columns) VALUES $values")
        except Exception as e:
            error_count +=1
            # logger.write_load_completion(documents=e, description='Bulk Time Entry Error')
        # logger.print_progress_bar(iteration=idx, total=record_count)
    p = "Out of {}".format(record_count)
    print("Errors while inserting time_entries: {err} ({p})".format(err=error_count, p=p))


@db_session
def insert_time_assignment_list(assignment_list):
    # Loop through every time assignment in the list and write to db
    record_count = len(assignment_list)
    error_count = 0
    # logger.print_progress_bar(iteration=0, total=record_count)
    for idx, assn in enumerate(assignment_list):
        columns = AsIs(','.join(assn.keys()))
        values = tuple([assn[column] for column in assn.keys()])
        try:
            db.execute("INSERT INTO public.time_assignment ($columns) VALUES $values")
        except Exception as e:
            error_count +=1
            # logger.write_load_completion(documents=e, description='Mass Time Assignment Error')
        # logger.print_progress_bar(iteration=idx, total=record_count)

    p = "Out of {}".format(record_count)
    print("Errors while inserting assignments: {err} ({p})".format(err=error_count, p=p))


@db_session
def copy_to_legacy_entries():
    db.execute("""INSERT INTO harvest_entries 
                  (SELECT te.id, te.hours, te.spent_date, te.billable, te.billable_rate, te.created_at, te.updated_at, 
                  te.entry_amount, te.person_id, te.person_name, te.project_id, te.project_name, te.project_code, 
                  te.client_id, te.client_name, te.task_id, te.task_name 
                  FROM time_entry te);""")


"""
READ:
The following fuctions fetch info from the db
"""


@db_session
def get_assignments_by_parent(parent_id):
    assns = select(a for a in Time_Assignment if a.parent_id == parent_id)[:]
    return assns


@db_session
def get_person_table():
    ppl_tbl = select(person for person in Person)[:]
    return ppl_tbl


@db_session
def get_person_by_id(identifier):
    """Search the person table for a matching Forecast or Harvest id and return the Data Warehouse object

    :param identifier: int - Forecast or Harvest id
    :return: full person record from db
    """
    person = Person.get(harvest_id=identifier)

    if person:
        pass
    else:
        person = Person.get(forecast_id=identifier)
    return person


@db_session
def get_project_table():
    proj_tbl = select(proj for proj in Project)[:]
    return proj_tbl


@db_session
def get_project_by_id(identifier):
    """Search the project table for a matching Forecast or Harvest id and return the Data Warehouse object

    :param identifier: int - Forecast or Harvest id
    :return: full client record from db
    """
    project = Project.get(harvest_id=identifier)

    if project:
        pass
    else:
        project = Project.get(forecast_id=identifier)
    return project


@db_session
def get_client_table():
    client_tbl = select(clnt for clnt in Client)[:]
    return client_tbl


@db_session
def get_client_by_id(identifier):
    """Search the client table for a matching Forecast or Harvest id and return the Data Warehouse object

    :param identifier: int - Forecast or Harvest id
    :return: full client record from db
    """
    client = Client.get(harvest_id=identifier)
    if client:
        pass
    else:
        client = Client.get(forecast_id=identifier)
    return client


@db_session
def get_time_entry_table():
    te_tbl = select(te for te in Time_Entry)[:]
    return te_tbl


@db_session
def get_updated_from_dates():
    mp = max(person.updated_at for person in Person)
    mpr = max(proj.updated_at for proj in Project)
    mc = max(cli.updated_at for cli in Client)
    mtsk = max(tsk.updated_at for tsk in Task)
    mte = max(te.updated_at for te in Time_Entry)
    mta = max(ta.updated_at for ta in Time_Assignment)
    return {'person': mp, 'project': mpr, 'client': mc, 'task': mtsk, 'time_entry': mte, 'time_assignment': mta}


"""
DELETE

These functions trunc or delete records
"""

@db_session
def trunc_legacy_entries():
    db.execute("TRUNCATE public.harvest_entries;")


"""
Utility Functions

These keep code above as clean and non-repetitive as possible.
"""


def calc_error_percent(record_count, error_count):
    percent = (error_count / record_count) * 100
    pretty_percent = str(percent) + '%'

    return pretty_percent