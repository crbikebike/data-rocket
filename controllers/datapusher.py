### Purpose of file: This is the controller that will interact with all main app files ###

## Imports
from controllers.dbcontroller import DataMaster

##  Classes
# This class receives data from the main app and calls its DataMaster to CRUD records
class DataActor(object):

    def __init__(self, is_test=False):
        if is_test == True:
            self.dm = DataMaster(is_test=True)
        else:
            self.dm = DataMaster()

    def create_tables(self):
        self.dm.create_entry_table()

    def drop_tables(self):
        self.dm.drop_entry_table()

    def close_conn(self):
        self.dm.dbconn.conn.close()

    def insert_dict(self, dict):
        self.columns = dict.keys()
        self.values = [dict[column] for column in self.columns]
        self.dm.insert_entry_table(self.columns, self.values)

    def insert_dict_list(self, dict_list):

        # Hope one day to replace this with the psycopg2.extras.execute_batch() method - will help with larger datasets
        for row in dict_list:
            self.columns = row.keys()
            self.values = [row[column] for column in self.columns]
            self.dm.insert_entry_table(self.columns, self.values)

