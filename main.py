# Imports
from controllers.datapusher import PusherBot
from controllers.utilitybot import *

"""
This file facilitates the pull and load of data
Current supported data sources: Harvest, Forecast
Planned support: Namely, Xero
"""

if __name__ == '__main__':

    # Make the PusherBot
    pb = PusherBot()

    # Load all data flagged as true and push to db
    pb.load_data(people=True, clients=True, tasks=True, projects=True, assignments=True, time_entries=True,
                 legacy_entries=True)