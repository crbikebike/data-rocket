"""
Makes sure that deletions in source systems are found and represented in the Data Warehouse
"""

# Imports
from controllers.datagrabber import Harvester, Forecaster
from controllers.ormcontroller import *
from controllers.utilitybot import datetime_format, date_format, datetime_format_ms, logger, full_load_datetime
from datetime import datetime, timedelta
import pandas as pd


# Classes
class GarbageCollector(object):
    """ Parent to all collectors these classes will find deleted entries in sources and remove from data warehouse."""

    def __init__(self):
        self.harv = Harvester()
        self.fore = Forecaster()

    @db_session
    def sync_people_records(self):
        """Compares source People records with Data Warehouse and removes records that were deleted from source"""
        people_list = select(p for p in Person)[:]
        harvest_people = self.harv.get_harvest_users(updated_since=full_load_datetime)['users']
        forecast_people = self.fore.get_forecast_people()['people']

        # Combine Forecast and Harvest people
        for f_person in forecast_people:
            pass

        # Make sets of pks from all three lists
        dw_people = {(p.harvest_id, p.forecast_id) for p in people_list}
        h_people =  {p['id'] for p in harvest_people}
        f_people = {p['id'] for p in forecast_people}
        pass