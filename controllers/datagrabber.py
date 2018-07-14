"""
File Purpose: This set of classes grabs data from various sources
Currently supported: Harvest, Forecast
"""

# Imports
import requests, json
from collections import deque
from datetime import datetime, timedelta
import calendar
from data_rocket_conf import config as conf


# Variables
forecast_account_id = conf['FORECAST_ACCOUNT_ID']
user_agent = conf['USER_AGENT']
auth_token = conf['HARVEST_AUTH']
harvest_account_id = conf['HARVEST_ACCOUNT_ID']
from_date = conf['FROM_DATE']


class Harvester(object):
    # Hits Harvest endpoints and returns their data
    def __init__(self, is_test=False):
        self.entry_per_page = 100
        self.harvest_base_url = 'https://api.harvestapp.com/v2/'
        self.harvest_headers = {}
        self.harvest_headers.update(Authorization=auth_token)
        self.harvest_headers.update({'Harvest-Account-ID': harvest_account_id})
        self.harvest_headers.update({'User-Agent': user_agent})
        self.harvest_params = {}
        self.harvest_params.update(page_per=self.entry_per_page)
        self.is_test = is_test

    """
    Utility Methods
    These reduce code duplication and help keep the core methods as clean as possible
    """

    def __filter_results__(self, results_dict, filter_list):
        # Filter results from larger dictionary by subtracting a set of its keys against a given list
        result_keys = set(results_dict.keys())
        filter_set = set(filter_list)
        # Compare the two sets, pop all keys that are not in the filter set
        pop_list = list(result_keys - filter_set)

        for pop_key in pop_list:
            results_dict.pop(pop_key)

        return results_dict


    def __flatten_results__(self, results_dict):
        """
        Cycle through a dict result with sub-dicts and return a flattened dict
        The fields we care about all have, id, name, and code as sub-keys
        """
        filter_list = ['id', 'name', 'code']
        # Copy dict so we can write/delete without affecting original while iterating
        flattened_dict = results_dict.copy()

        # Find each parent key:value that is a dict and flatten
        for pk, pv in results_dict.items():
            if type(pv) is dict:
                pv = self.__filter_results__(results_dict=pv, filter_list=filter_list)
                flattened_fields = {'{pk}_{sk}'.format(pk=pk, sk=sk): sv
                                    for sk, sv in pv.items()}
                flattened_dict.update(flattened_fields)
                flattened_dict.pop(pk)
            elif type(pv) is list:

                json_pv = json.dumps(pv)
                flattened_dict.update(roles=json_pv)
            else:
                # If the value isn't a dict, do nothing
                pass

        return flattened_dict

    def __get_request__(self, api_url, extra_params=''):
        """
        Will hit a harvest API and return result as json object - Called for each paginated result in Harvest API
        """
        full_url = self.harvest_base_url + api_url
        headers = self.harvest_headers
        params = self.harvest_params
        params.update(extra_params)

        try:
            r = requests.get(url=full_url, headers=headers, params=params)
            json_r = json.loads(r.text)
        except Exception as e:
            json_r = {}
            print('Could not hit endpoint {ep} because {e}'.format(ep=api_url, e=e))

        return json_r

    def __get_api_data__(self, root_key, extra_params='', filters=''):
        """
        Accepts params for Harvest endpoint and returns result set. Meant to be portable for all v2 endpoints.
        """
        api_params = {}
        api_params.update(extra_params)
        api_json_result = self.__get_request__(api_url=root_key, extra_params=api_params)

        # Get page numbers, build queue
        page_qty = api_json_result['total_pages']
        page_queue = deque(range(1, (page_qty + 1)))

        # Process the queue until empty
        api_list = []
        id_list = [] # Keep track of ids added to list to prevent inserting multiple of the same record
        api_json_result.update(id_list=id_list)

        print('Starting {} Harvest Pull'.format(root_key.capitalize()))
        while len(page_queue) > 0:
            page_num = page_queue.popleft()
            api_params.update(page=page_num)
            # Request api load for the current page
            page_json_result = self.__get_request__(api_url=root_key, extra_params=api_params)
            api_entities = page_json_result[root_key]
            print(
                'Processing Page: ' + str(page_json_result['page']) + ' out of ' + str(page_json_result['total_pages']))

            # If there are keys to filter, do that. Otherwise just add the entire resposne to the api_list
            for entity in api_entities:
                if entity['id'] not in api_json_result['id_list']:
                    entity = self.__filter_results__(results_dict=entity, filter_list=filters)
                    # Some results have sub-dictionaries so we want to flatten them
                    flat_entity = self.__flatten_results__(entity)
                    api_list.append(flat_entity)
                    id_list.append(flat_entity['id'])
                else:
                    pass

        # Replace the endpoint data with our updated info
        api_json_result.update({root_key: api_list})

        return api_json_result

    """
    All methods below start with get_ correspond to a Harvest v2 API endpoint by the same name as the root_key var
    Each has a filter list that cuts the number of fields down to what is important for the data warehouse
    """

    def get_harvest_time_entries(self, updated_since):
        root_key = 'time_entries'
        filters = ['id', 'spent_date', 'hours', 'billable', 'billable_rate', 'created_at', 'updated_at',
                   'user', 'client', 'project', 'task']

        # Setup the endpoint params
        time_entry_params = {}
        time_entry_params.update(updated_since=updated_since) # Only pull entires updated since this date
        time_entry_params.update({'from': from_date}) # If above param isn't present, will pull from this date
        time_entry_params.update({'is_running': 'false'}) # Prevent pulling running timers

        # Perform data pull
        time_entry_dict = self.__get_api_data__(root_key=root_key, filters=filters,
                                                extra_params=time_entry_params)
        return time_entry_dict

    def get_harvest_users(self, updated_since):
        root_key = 'users'
        person_params = {}
        person_params.update(updated_since=updated_since)  # Only pull entires updated since this date
        filters = ['id', 'first_name', 'last_name', 'email', 'timezone', 'weekly_capacity', 'is_contractor',
                   'is_active', 'roles', 'avatar_url', 'created_at', 'updated_at']

        # Perform data pull
        users_dict = self.__get_api_data__(root_key=root_key, filters=filters, extra_params=person_params)
        return users_dict

    def get_harvest_clients(self, updated_since):
        root_key = 'clients'
        client_params = {}
        client_params.update(updated_since=updated_since)  # Only pull entires updated since this date
        filters = ['id', 'name', 'is_active', 'created_at', 'updated_at']

        # Perform data pull
        clients_dict = self.__get_api_data__(root_key=root_key, filters=filters, extra_params=client_params)
        return clients_dict

    def get_harvest_projects(self, updated_since):
        root_key = 'projects'
        project_params = {}
        project_params.update(updated_since=updated_since)  # Only pull entires updated since this date
        filters = ['id', 'name', 'code', 'is_active', 'is_billable', 'budget', 'budget_is_monthly',
                   'created_at', 'updated_at', 'starts_on', 'ends_on', 'client']

        # Perform data pull
        projects_dict = self.__get_api_data__(root_key=root_key, filters=filters, extra_params=project_params)
        return projects_dict

    def get_harvest_tasks(self, updated_since):
        root_key = 'tasks'
        task_params = {}
        task_params.update(updated_since=updated_since)  # Only pull entires updated since this date
        filters = ['id', 'name', 'updated_at']

        # Perform data pull
        tasks_dict = self.__get_api_data__(root_key=root_key, filters=filters, extra_params=task_params)
        return tasks_dict


class Forecaster(object):
    # Grabs Forecast data - Note that the Forecast API is not officially supported
    def __init__(self, is_test=False):
        self.forecast_base_url = 'https://api.forecastapp.com/'
        self.forecast_headers = {}
        self.forecast_headers.update(Authorization=auth_token)
        self.forecast_headers.update({'Forecast-Account-ID': forecast_account_id})
        self.forecast_headers.update({'User-Agent': user_agent})
        self.forecast_params = {}
        self.date_string = '%Y-%m-%d'
        self.is_test = is_test

    """
    Utility Methods
    These reduce code duplication and help keep the core methods as clean as possible
    """

    def __get_api_data__(self, api_url, extra_params=''):
        """
        Hits the Forecast endpoint specified by the api_url param
        """
        full_url = self.forecast_base_url + api_url
        headers = self.forecast_headers
        params = self.forecast_params
        params.update(extra_params)

        # Perform api request
        try:
            r = requests.get(url=full_url, headers=headers, params=params)
            api_json_result = json.loads(r.text)
        except Exception as e:
            print('Attempt for Forecast {url} failed because {e}'.format(url=api_url, e=e))

        return api_json_result

    def __filter_results__(self, results_dict, filter_list):
        """
        Filter results from larger dictionary by subtracting a set of its keys against a given list
        """
        result_keys = set(results_dict.keys())
        filter_set = set(filter_list)

        # Compare the two sets, pop all keys that are not in the filter set
        pop_list = list(result_keys - filter_set)
        for pop_key in pop_list:
            results_dict.pop(pop_key)

        return results_dict

    def __get_forecast_dates__(self):
        """
        Gets the first date of the previous month and last day of 3 months from now
        This is used to see how much Forecast history and future to pull
        """
        if self.is_test:
            past_dates = 2
            future_dates = 3
        else:
            past_dates = 30
            future_dates = 90

        now = datetime.now()
        # Get the first date of the past month
        time_machine = now - timedelta(days=past_dates)
        look_behind = time_machine.strftime('%Y-%m-01')
        # Get the last date of 6-ish months from now
        future_machine = now + timedelta(days=future_dates)
        eom = calendar.monthrange(future_machine.year, future_machine.month)[1]
        look_ahead = future_machine.strftime('%Y-%m-{d}'.format(d=eom))

        return look_behind, look_ahead

    """
    The below fuctions pull data from the Forecast API
    There is also a set of filters for each endpoint to limit fields being sent downstream
    """

    def get_forecast_projects(self):
        print('Getting Forecast Projects')
        api_url = 'projects'
        filters = ['id', 'name', 'code', 'start_date', 'end_date', 'harvest_id', 'client_id',
                   'updated_at']
        projects_json_result = self.__get_api_data__(api_url)

        for project in projects_json_result['projects']:
            # Filter the results to only the fields we care about
            project = self.__filter_results__(results_dict=project, filter_list=filters)
            project.update(starts_on=project.pop('start_date')) # Update to match the Harvest Field Name
            project.update(ends_on=project.pop('end_date'))
        return projects_json_result

    def get_forecast_people(self):
        print('Getting Forecast People')
        api_url = 'people'
        filters = ['id','harvest_user_id','first_name', 'last_name', 'email', 'updated_at', 'archived']
        people_json_result = self.__get_api_data__(api_url)

        for person in people_json_result['people']:
            # Filter the results to only the fields we care about
            person = self.__filter_results__(results_dict=person, filter_list=filters)
            person.update(harvest_id = person.pop('harvest_user_id')) # Update to match the Harvest Field Name
        return people_json_result

    def get_forecast_assignments(self):
        print('Getting Forecast Assignments')
        api_url = 'assignments'
        start_date, end_date = self.__get_forecast_dates__()
        extra_params = {'start_date': start_date, 'end_date': end_date}
        filters = ['id','start_date','end_date','allocation','updated_at','project_id','person_id']
        assignment_json_result = self.__get_api_data__(api_url, extra_params=extra_params)

        for assn in assignment_json_result['assignments']:
            # Filter results to the fields we care about
            assn = self.__filter_results__(results_dict=assn, filter_list=filters)
        return assignment_json_result

    def get_forecast_clients(self):
        print('Getting Forecast Clients')
        api_url = 'clients'
        filters = ['id', 'name', 'harvest_id', 'updated_at']
        client_json_result = self.__get_api_data__(api_url)

        for client in client_json_result['clients']:
            # Filter the results to only the fields we care about
            client = self.__filter_results__(results_dict=client, filter_list=filters)

        return client_json_result