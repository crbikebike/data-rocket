# Imports
from controllers.datapusher import PusherBot

### Main Function - All the work facilitated from here.
if __name__ == '__main__':

    # Load data into a list of dicts
    loaded_data = PusherBot.load_data()

    # Push that data to the db
    PusherBot.push_data(loaded_data=loaded_data)