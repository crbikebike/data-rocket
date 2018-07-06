### Purpose of file: This is the controller that will interact with all main app files ###

## Imports
import controllers.ormcontroller as orm

##  Classes
# This class needs to be remade to take the clutter from the main.py file
class DataActor(object):

    def __init__(self, is_test=False):
        if is_test == True:
            pass
        else:
            pass

    """
    ### Needed Functions ###

    Find existing rows, update if needed - probably only look back 30 days since the entries get locked

    Use the Harvest Param to only pull since last update, only pull things after that entry 
        - Until that, will just drop the table and re-insert everything

    Count rows being inserted - give status when, catch error and note where it left off
    """