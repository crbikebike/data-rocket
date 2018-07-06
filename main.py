# Imports
from controllers.datamunger import Munger
from data_rocket_conf import config as conf
import controllers.ormcontroller as orm

# grab config vars to pass to our classes later
forecast_account_id = conf['FORECAST_ACCOUNT_ID']
user_agent = conf['USER_AGENT']
auth_token = conf['HARVEST_AUTH']
harvest_account_id = conf['HARVEST_ACCOUNT_ID']
from_date = conf['FROM_DATE']


### Needed Functions ###

## Find existing rows, update if needed - probably only look back 30 days since the entries get locked

## Find most recent entryID, only pull things after that entry - Until that, will just drop the table and re-insert everything

## Count rows being inserted - give status, catch error and note where it left off


### Main Function - All the work happens here.
if __name__ == '__main__':

    # Instantiate our friendly data Munger
    mungy = Munger()

    # Place Munged Harvest entries into a dictionary
    actuals = mungy.get_munged_harvest_time_entries()

    # Get the list of time entries to insert, send them to the DataActor
    time_entry_list = actuals['time_entries']
    orm.insert_time_entry_list(time_entry_list)
