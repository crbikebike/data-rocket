# Imports
from controllers.datamunger import Munger
import controllers.ormcontroller as orm

### Main Function - All the work facilitated from here.
if __name__ == '__main__':

    # Instantiate our friendly data Munger
    mungy = Munger()

    # Place Munged Harvest entries into a dictionary
    time_entries = mungy.munge_harvest_time_entries()
    people = mungy.munge_person_list()
    clients = mungy.munge_client_list()
    tasks = mungy.munge_task_list()
    projects = mungy.munge_project_list()

    print('Inserting People DB records')
    # Insert people
    people_list = people['users']
    orm.insert_people_list(people_list)

    print('Inserting Client DB records')
    # Insert clients
    client_list = clients['clients']
    orm.insert_clients_list(client_list)

    print('Inserting Project DB records')
    # Insert projects
    project_list = projects['projects']
    orm.insert_projects_list(project_list)

    print('Inserting Task DB records')
    # Insert tasks
    tasks_list = tasks['tasks']
    orm.insert_tasks_list(tasks_list)

    print('Inserting Time Entry DB records')
    # Get the list of time entries to insert, send them to the DataActor
    time_entry_list = time_entries['time_entries']
    orm.insert_time_entries_list(time_entry_list)
