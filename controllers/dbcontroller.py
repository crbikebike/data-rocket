"""
All DB read/write actions stored here
"""

import psycopg2
from psycopg2.extensions import AsIs
from urllib.parse import urlparse
import data_rocket_conf
from controllers.sqlstatements import sqldict, sqldict_test, test_data
from datetime import timedelta


## This class establishes connection with the database
class DataConnection(object):
    def __init__(self):
        self.cur = None
        self.conn = self._get_db_conn()
        self.cur = self._get_db_cursor()


    def _get_db_conn(self):
        cfg = data_rocket_conf.config
        url_cfg = cfg['DB_CONN']
        # urlparse.uses_netloc.append("postgres")
        url = urlparse(url_cfg)

        try:
            conn = psycopg2.connect(
                database=url.path[1:],
                user=url.username,
                password=url.password,
                host=url.hostname,
                port=url.port
            )
            return conn

        except Exception as e:
            print("failed to connect, {}".format(e))


    def _get_db_cursor(self):
        if self._has_connect():
            if self._has_cursor():
                return self.cur

            else:
                try:
                    cur = self.conn.cursor()
                    return cur
                except Exception as e:
                    print('could not get cursor because {}'.format(e))

        else:
            return False


    def _has_connect(self):
        if self.conn is None or self.conn.closed > 0:
            return False
        elif self.conn.closed == 0:
            return True
        else:
            return Exception


    def _has_cursor(self):
        if self.cur is None:
            return False
        elif self.cur.closed == False:
            return False
        elif self.cur == True:
            return True
        else:
            return Exception


## This class interacts with the database directly by calling queries stored in the sqlstatements file
class DataMaster(object):

    def __init__(self,is_test=False):
        self.dbconn = DataConnection()

        if is_test == True:
            self.sql = sqldict_test
            self.drop_entry_table()
            self.create_entry_table()

            # Fill with Test Data
            for element in test_data:
                pass
                cur = self.dbconn.cur
                cur.execute("Insert your INSERT statement here")

            self.dbconn.conn.commit()
            print ('test data is blank - please configure by setting up test dictionary in sqlstatements.py')

        else:
            self.sql = sqldict
            self.create_entry_table()
            print('created DataMaster')


    def create_entry_table(self):
        statement = self.sql['create_entry_table']
        try:
            self.dbconn.cur.execute(statement)
            self.dbconn.conn.commit()
            print('created entry table')
            return True

        except Exception as e:
            self.dbconn.conn.close()
            print('could not create table because {}'.format(e))


    def drop_entry_table(self):
        statement = self.sql['drop_entry_table']
        try:
            self.dbconn.cur.execute(statement)
            self.dbconn.conn.commit()
            print('dropped entry table')
            return True

        except Exception as e:
            self.dbconn.conn.close()
            print('could not drop table because {}'.format(e))


    def insert_entry_table(self, columns, values):
        statement = self.sql['insert_entry_table']
        try:
            self.as_is_cols = AsIs(','.join(columns))
            self.tuple_vals = tuple(values)
            #mogrify reduces risk of sql injection
            self.moggy = self.dbconn.cur.mogrify(statement, (self.as_is_cols, self.tuple_vals))
            self.dbconn.cur.execute(self.moggy)
            self.dbconn.conn.commit()
            return True

        except Exception as e:
            self.dbconn.conn.close()
            print(self.moggy[:40])
            print('could not insert table because {}'.format(e))

