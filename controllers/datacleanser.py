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

        # Transform into Pandas friendly list of k:v
        harvest_people = [{'hid': p['id']} for p in harvest_people]
        forecast_people = [{'fid': p['id'], 'hid': p['harvest_id']} for p in forecast_people]

        # Combine Forecast and Harvest people using Pandas Data Frames
        hdf = pd.DataFrame(harvest_people)
        fdf = pd.DataFrame(forecast_people)
        combined = fdf.merge(hdf, how='outer', on='hid', suffixes=('_fore', '_harv'))

        # Convert the combined DataFrame back into a list of dictionaries
        combined_dict_list = combined.T.apply(lambda x: x.fillna(value=0).to_dict()).tolist()

        # Update the Pandas NaN fields to None to match Data Warehouse set
        for dict in combined_dict_list:
            for k, v in dict.items():
                if v == 0.0:
                    dict.update({k: None})

        # Make sets to compare the two data sources
        dw_people = {(p.forecast_id, p.harvest_id) for p in people_list}
        combined_set = {(r['fid'], r['hid']) for r in combined_dict_list}

        # Subtract the sets and the remainder is your list to delete
        delete_set = dw_people - combined_set

        """To finish: get data warehouse pk's from the set above and delete them"""
        pass