### Purpose of file: This is the controller that will perform file data gathering and push to the db ###

## Imports
from controllers.datamunger import UberMunge
from controllers.datacleanser import GarbageCollector
from controllers.utilitybot import logger


##  Classes


class PusherBot(object):
    # This class handles collating and pushing clean data to the DB
    gc = GarbageCollector()
    def __init__(self, is_test=False):
        self.uber = UberMunge(is_test=is_test)

    def load_data(self, full_load=False, all_tables=False, people=False, clients=False, tasks=False, projects=False,
                  assignments=False, time_entries=False):

        if full_load:
            # If full_load, set dates far in the past to ensure all data is grabbed
            self.uber.set_load_dates(is_full_load=True)
        else:
            # If differential, grab max date from existing records
            self.uber.set_load_dates(is_full_load=False)

        """
        For each flag, check if enabled and then process if true
        """

        if people or all_tables:
            self.uber.munge_person()

        if clients or all_tables:
            self.uber.munge_client()

        if tasks or all_tables:
            self.uber.munge_task()

        if projects or all_tables:
            self.uber.munge_project()

        if assignments or all_tables:
            self.uber.munge_assignment()

        if time_entries or all_tables:
            self.uber.munge_time_entries()

        # Run cleanup routines on Time Entry and Time Assignments to remove deleted source items from data warehouse
        self.gc.sync_forecast_assignments()
        self.gc.sync_harvest_time_entries()

