# Imports
from controllers.datapusher import PusherBot
from sys import argv
from controllers.utilitybot import *
from data_rocket_conf import config as drc

"""
This file facilitates the pull and load of data
Current supported data sources: Harvest, Forecast
Planned support: Namely, Xero
"""
config = process_args(argv)

if __name__ == '__main__':

    # Make the PusherBot
    pb = PusherBot(is_test=drc['DEBUG'])

    # Load all data flagged as true and push to db
    pb.load_data(full_load=config['full_load'], people=config['people'], clients=config['clients'],
                 tasks=config['tasks'], projects=config['projects'], assignments=config['assignments'],
                 time_entries=['time_entries'])


    write_load_completion(argv[1:])
