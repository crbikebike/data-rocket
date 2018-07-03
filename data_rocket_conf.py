### All environment vars are defined in Heroku
import os

config = {'DEBUG': False,
          'DB_CONN': os.environ.get('DB_CONN'),
          'HARVEST_AUTH': os.environ.get('HARVEST_AUTH'),
          'FORECAST_ACCOUNT_ID': os.environ.get('FORECAST_ACCOUNT_ID'),
          'HARVEST_ACCOUNT_ID': os.environ.get('HARVEST_ACCOUNT_ID'),
          'USER_AGENT': os.environ.get('USER_AGENT'),
          'FROM_DATE': os.environ.get('FROM_DATE')}