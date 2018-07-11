"""
Some simple functions that don't have a more natural home
"""
from controllers.ormcontroller import write_rocket_log
from datetime import datetime

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

def write_load_completion(documents):
    description = 'load completed'
    now = datetime.now()
    success = True
    docs = documents

    write_rocket_log(description=description, timestamp=now, success=success, documents=documents)
