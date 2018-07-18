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
        # Get your Harv/Forecast People
        hpeeps = self.harv.get_harvest_users(updated_since=full_load_datetime)['users']
        fpeeps = self.fore.get_forecast_people()['people']
        
        # Get Data Warehouse people
        people_list = select(p for p in Person)[:]

        # Make list of dicts for harv/forecast people
        h_people = [{'hid': p['id']} for p in hpeeps]
        f_people = [{'fid': p['id'], 'hid': p['harvest_id']} for p in fpeeps]

        # Load them into Data Frames
        hdf = pd.DataFrame(h_people)
        fdf = pd.DataFrame(f_people)

        # Merge the two Data Frames into one
        dfcombined = fdf.merge(hdf, how='outer', on='hid', suffixes=('_fore', '_harv'))

        # Make list of dicts from Data Warehouse people
        dw_people_list = [{'dwid': p.id, 'fid': p.forecast_id, 'hid': p.harvest_id} for p in people_list]

        # Load into Data Frame
        dwdf = pd.DataFrame(dw_people_list)

        # Create Super list of all Data Warehouse records, Minor list of all Source records
        dfsuper = dwdf.merge(dfcombined, how='left', on=['hid', 'fid'])
        print(dfsuper)
        dfminor = dwdf.merge(dfcombined, how='right', on=['hid', 'fid'])
        print(dfminor)

        # Convert those lists into list of tuples so they can be converted into Sets
        super_list = dfsuper.T.apply(lambda x: x.fillna(value=0).to_dict()).tolist()
        super_list = [(dict['dwid'], dict['fid'], dict['hid']) for dict in super_list]
        minor_list = dfminor.T.apply(lambda x: x.fillna(value=0).to_dict()).tolist()
        minor_list = [(dict['dwid'], dict['fid'], dict['hid']) for dict in minor_list]

        # Subtract the Minor set from the Super set.  That's your set of records that are extra in the DW
        dfsubtractor = set(super_list) - set(minor_list)
        print('these are the extra records', dfsubtractor)

        """To finish: get data warehouse pk's from the set above and delete them"""
        pass