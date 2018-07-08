# Imports
from controllers.datapusher import PusherBot

"""
This file facilitates the pull and load of data
Current supported data sources: Harvest
Planned support: Forecast, Namely, Xero
"""

if __name__ == '__main__':

    # Make the PusherBot
    pb = PusherBot()

    # Load all data flagged as true and push to db
    pb.load_data(people=True, clients=True, tasks=True)
