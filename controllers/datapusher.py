### Purpose of file: This is the controller that will perform file data gathering and push to the db ###

## Imports
from controllers.ormcontroller import *
from controllers.datamunger import Munger, UberMunge
from controllers.utilitybot import logger

"""
### Needed Functions ###
Use the Harvest Param to only pull since last update, only pull things after that entry 
    - Until that, will just drop the table and re-insert everything
    - One way - track the id's added to the results from data grabber, have munger check if that key exists in the db, 
    then pop from the api return list before inserting.  
    - More complicated and efficient way: Replace the munger for/if loops with compares of a sqlite db that represents 
    the data warehouse.
"""

##  Classes


class PusherBot(object):
    # This class handles collating and pushing clean data to the DB
    def __init__(self, is_test=False):
        self.mungy = Munger(is_test=is_test)
        self.uber = UberMunge(is_test=is_test)

    def load_data(self, full_load=False, people=False, clients=False, tasks=False, projects=False, assignments=False,
                  time_entries=False):

        if full_load:
            # If full load, drop all and create
            #db.drop_all_tables(with_all_data=True)
            #db.create_tables()
            self.mungy.set_load_dates(is_full_load=True)
            self.uber.set_load_dates(is_full_load=True)
        else:
            # If differential, don't drop tables and use the updated_from dates
            self.mungy.set_load_dates(is_full_load=False)
            self.uber.set_load_dates(is_full_load=False)

        """
        For each flag, check if enabled and then process if true
        """
        
        if people or full_load:
            self.uber.munge_person()

        if clients or full_load:
            self.uber.munge_client()

        if tasks or full_load:
            self.uber.munge_task()

        if projects or full_load:
            self.uber.munge_project()

        if assignments or full_load:
            self.assignments_list = self.mungy.munge_forecast_assignments()
            print('Inserting Forecast Assignment DB records')
            insert_time_assignment_list(self.assignments_list)

        if time_entries or full_load:
            self.munged_time_entries = self.mungy.munge_harvest_time_entries()
            print('Inserting Time Entry DB records')
            self.time_entries_list = self.munged_time_entries['time_entries']
            insert_time_entries_list(self.time_entries_list)

            # Legacy Entry support
            self.legacy_time_entries_list = self.mungy.munge_legacy_harvest_entries(self.time_entries_list)
            print('Inserting Legacy Harvest Entry DB records')
            insert_legacy_time_entries_list(self.legacy_time_entries_list)