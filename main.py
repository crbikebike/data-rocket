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

    # Load data into a list of dicts
    loaded_data = pb.load_data()

    # Push that data to the db
    pb.push_data(loaded_data=loaded_data)