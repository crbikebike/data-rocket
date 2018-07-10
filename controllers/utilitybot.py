"""
Some simple functions to help with logging and such that don't have a more natural home
"""

def insert_log(description, datetime, success, documents_list):
    pass

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

        start_string = 'Starting {} data load for '.format(', '.join([s for s in start_list]))
        print(start_string)
        return config

    else: #If started without any params, do full load
        config.update(full_load=True)
        print('Starting full_load data load')
        return config