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
        df_combined = fdf.merge(hdf, how='outer', on='hid', suffixes=('_fore', '_harv'))

        # Make list of dicts from Data Warehouse people
        dw_people_list = [{'dwid': p.id, 'fid': p.forecast_id, 'hid': p.harvest_id} for p in people_list]

        # Load into Data Frame
        dw_df = pd.DataFrame(dw_people_list)

        # Create Super list of all Data Warehouse records, Minor list of all Source records
        df_super = dw_df.merge(df_combined, how='left', on=['hid', 'fid'])
        df_minor = dw_df.merge(df_combined, how='right', on=['hid', 'fid'])

        # Convert those lists into list of dicts, then list of tuples so they can be converted into Sets
        super_list = df_super.T.apply(lambda x: x.fillna(value=0).to_dict()).tolist()
        super_list = [(dict['dwid'], dict['fid'], dict['hid']) for dict in super_list]
        minor_list = df_minor.T.apply(lambda x: x.fillna(value=0).to_dict()).tolist()
        minor_list = [(dict['dwid'], dict['fid'], dict['hid']) for dict in minor_list]

        # Subtract the Minor set from the Super set.  That's your set of records that are extra in the DW
        dfsubtractor = set(super_list) - set(minor_list)

        # Loop through the set and delete each entry
        print("Purging Deleted People ({}) Records".format(len(dfsubtractor)))
        for entity in dfsubtractor:
            try:
                e_id = int(entity[0])
                Person[e_id].delete()
            except Exception as e:
                logger.write_load_completion(str(e), "Fail while deleting person", success=False)


    @db_session
    def sync_forecast_assignments(self):
        """Finds deleted entries from Forecast and removes them from the Data Warehouse"""
        # Get Source Data. Create list of dicts.
        forecast_assignments = self.fore.get_forecast_assignments()['assignments']
        assn_list = [{'fid': assn['id']} for assn in forecast_assignments]

        # Get Data Warehouse Data. Create list of dicts.
        dw_assignments = select(a for a in Time_Assignment)[:]
        dw_assns = [{'dw_id': a.id, 'fid': a.parent_id} for a in dw_assignments]

        # Make DataFrames of each
        df_fore = pd.DataFrame(assn_list)
        df_dw = pd.DataFrame(dw_assns)

        # Merge to make Super and Minor lists
        df_super = df_dw.merge(df_fore, how='left', on='fid')
        df_minor = df_dw.merge(df_fore, how='right', on='fid')

        # Make lists of dicts, transform into sets of tuples
        super_list = df_super.T.apply(lambda x: x.fillna(value=0).to_dict()).tolist()
        super_list = [(dict['dw_id'], dict['fid']) for dict in super_list]
        minor_list = df_minor.T.apply(lambda x: x.fillna(value=0).to_dict()).tolist()
        minor_list = [(dict['dw_id'], dict['fid']) for dict in minor_list]

        # Subtract the sets of super and minor list
        df_subtractor = set(super_list) - set(minor_list)

        # Loop through each entity in the set and delete from the Data Warehouse
        print("Purging Deleted Time Assignments ({}) Records".format(len(df_subtractor)))
        for entity in df_subtractor:
            try:
                e_id = int(entity[0])
                Time_Assignment[e_id].delete()
            except Exception as e:
                logger.write_load_completion(str(e), "Fail while deleting time assignment", success=False)


    @db_session
    def sync_harvest_time_entries(self):
        """Finds deleted entries from Harvest and removes them from the Data Warehouse"""
        # Set your updated date so you only need to pull a subset of time entries
        updated_since = datetime.now() - timedelta(days=21)
        updated_since_str = datetime.strftime(updated_since, datetime_format_ms)

        # Get Source Data. Create list of dicts.
        harv_entries = self.harv.get_harvest_time_entries(updated_since=updated_since_str)['time_entries']
        harv_entries = [{'id': entry['id']} for entry in harv_entries]

        # Get Data Warehouse Data. Create list of dicts.
        dw_entries = select(te for te in Time_Entry if te.updated_at > updated_since)[:]
        dw_entries = [{'id': te.id} for te in dw_entries]

        # Make DataFrames of each
        df_harv = pd.DataFrame(harv_entries)
        df_dw = pd.DataFrame(dw_entries)

        # Merge to make Super and Minor lists
        df_super = df_dw.merge(df_harv, how='left', on='id')
        df_minor = df_dw.merge(df_harv, how='right', on='id')

        # Make lists of dicts, transform into sets of tuples
        super_list = df_super.T.apply(lambda x: x.fillna(value=0).to_dict()).tolist()
        super_list = [(dict['id']) for dict in super_list]
        minor_list = df_minor.T.apply(lambda x: x.fillna(value=0).to_dict()).tolist()
        minor_list = [(dict['id']) for dict in minor_list]

        # Subtract the sets of super and minor list
        df_subtractor = set(super_list) - set(minor_list)

        # Loop through each entity in the set and delete from the Data Warehouse
        print("Purging Deleted Time Entries ({}) Records".format(len(df_subtractor)))
        for entity in df_subtractor:
            try:
                e_id = int(entity)
                Time_Entry[e_id].delete()
            except Exception as e:
                logger.write_load_completion(str(e), "Fail while deleting time entry", success=False)