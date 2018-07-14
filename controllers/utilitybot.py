"""
Some simple functions that don't have a more natural home
"""
from controllers.ormcontroller import write_rocket_log
from datetime import datetime
datetime_string = '%Y-%m-%dT%H:%M:%SZ'
date_string = '%Y-%m-%d'

class LoggerBot(object):
    """
    LoggerBot handles tracking record counts, messaging to console, and sending logs to the datarocketlog table
    """
    def __init__(self):
        self.load_success = False
        self.load_description = ''
        self.load_start = datetime.now()
        self.load_end = datetime.strptime('1984-12-03T00:00:00Z', datetime_string)
        self.load_documents = []

    def start_load_msg(self, entity_name, load_list):
        record_count = len(load_list)
        print("Found {qty} {en} records.  Starting Load.".format(qty=record_count, en=entity_name))

    def end_load_msg(self, entity_name, load_list):
        pass

    def write_load_completion(self, documents, description='load completed', success=False):
        now = datetime.now()
        docs = documents
        success = self.load_success

        write_rocket_log(description=description, timestamp=now, success=success, documents=docs)


def process_args(argv):
    """
    Processes arguments sent with command line start and returns dict with config info
    """
    config = {'full_load': False,
              'people': False,
              'clients': False,
              'tasks': False,
              'projects': False,
              'assignments': False,
              'time_entries': False,}

    load_list = ['full_load', 'people', 'clients', 'tasks', 'projects', 'assignments', 'time_entries']

    if len(argv) > 1:
        args = argv[1:]
        start_list = []
        for item in load_list:
            if item in args:
                config.update({item: True})
                start_list.append(item)

        start_string = 'Starting {} data load'.format(', '.join([s for s in start_list]))
        print(start_string)
        return config

    else: #If started without any params, do diff load
        config.update(full_load=False)
        print('Starting diff data load')
        return config


""" Instantiate a logger bot for the other files to use"""
logger = LoggerBot()