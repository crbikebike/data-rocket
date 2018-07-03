# Imports
from controllers.datapusher import DataActor
from controllers.datagrabber import Harvester, Forecaster
from data_rocket_conf import config as conf


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

    # Instantiate harv the friendly Harvest API grabber
    harv = Harvester(auth_token=auth_token,harvest_account_id=harvest_account_id,
                     user_agent=user_agent,from_date=from_date)
    # Place Harvest entries into a dictionary
    actuals = harv.harvest_actuals()

    # Instantiate a DataActor - Our friend that will talk to PostgreSQL  :)
    da = DataActor()

    # Drop and create the entry table - no graceful way to do a diff at the moment
    da.drop_tables()
    da.create_tables()

    # Loop through each time entry in the dictionary and insert the row into the db
    for entry in actuals['time_entries']:
        da.insert_dict(dict=entry)

    # Tidy up before leaving - close DB connection
    da.close_conn()
