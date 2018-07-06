### File Purpose: This set of classes grabs data from various sources ###

# Imports
import requests, json
from collections import deque
import datetime

# Hits Harvest endpoints and returns their data
class Harvester(object):

    def __init__(self, auth_token, harvest_account_id, user_agent, is_test=False):
        self.entry_per_page = 100
        self.harvest_base_url = 'https://api.harvestapp.com/v2/'
        self.harvest_headers = {}
        self.harvest_headers.update(Authorization=auth_token)
        self.harvest_headers.update({'Harvest-Account-ID': harvest_account_id})
        self.harvest_headers.update({'User-Agent': user_agent})
        self.harvest_params = {}
        self.harvest_params.update(page_per=self.entry_per_page)
        self.is_test = is_test

    # Filter results from larger dictionary by subtracting a set of its keys against a given list
    def __filter_results__(self, results_dict, filter_list):
        result_keys = set(results_dict.keys())
        filter_set = set(filter_list)

        # Compare the two sets, pop all keys that are not in the filter set
        pop_list = list(result_keys - filter_set)
        for pop_key in pop_list:
            results_dict.pop(pop_key)

        return results_dict

    # Cycle through a dict result with sub-dicts and return a flattened dict
    def __flatten_results__(self, results_dict):
        # The fields we care about all have, id, name, and code as sub-keys
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
                # enum_pv = dict(enumerate(pv))
                json_pv = json.dumps(pv)
                flattened_dict.update(roles=json_pv)
            else:
                # If the value isn't a dict, do nothing
                pass

        return flattened_dict

    # Will hit a harvest API and return result as json object - Called for each paginated result in Harvest API
    def __get_request__(self, api_url, extra_params={}):
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

    # Accepts params for Harvest endpoint and returns result set. Meant to be portable for all v2 endpoints.
    def __get_api_data__(self, root_key, extra_params={}, filters=[]):
        api_params = {}
        api_params.update(extra_params)
        api_json_result = self.__get_request__(api_url=root_key, extra_params=api_params)

        # Get page numbers, build queue
        page_qty = api_json_result['total_pages']
        page_queue = deque(range(1, (page_qty + 1)))

        api_list = []
        print('Starting {} Harvest Pull'.format(root_key.capitalize()))
        while len(page_queue) > 0:
            page_num = page_queue.popleft()
            api_params.update(page=page_num)
            page_json_result = self.__get_request__(api_url=root_key, extra_params=api_params)
            api_entities = page_json_result[root_key]
            print(
                'Processing Page: ' + str(page_json_result['page']) + ' out of ' + str(page_json_result['total_pages']))

            # If there are keys to filter, do that. Otherwise just add the entire resposne to the api_list
            for entity in api_entities:
                if filters:
                    entity = self.__filter_results__(results_dict=entity, filter_list=filters)
                else:
                    # Don't flter, just pass on the whole entity
                    pass

                # Some results have sub-dictionaries so we want to flatten them
                flat_entity = self.__flatten_results__(entity)
                api_list.append(flat_entity)

        # Replace the endpoint data with our updated info
        api_json_result.update({root_key: api_list})

        return api_json_result

    """
    All Functions below starting with get_ correspond to a Harvest v2 API endpoint by the same name as the root_key var
    They utilize the above functions to get data and do light formatting to prepare for db entry
    """

    def get_harvest_time_entries(self, from_date):
        root_key = 'time_entries'
        # Name the keys you care about from the api
        filters = ['id', 'spent_date', 'hours', 'billable', 'billable_rate', 'created_at', 'updated_at',
                   'user', 'client', 'project', 'task']
        time_entry_params = {}

        # If in test mode, only pull the most recent 2 days
        if self.is_test:
            now = datetime.datetime.now()
            timemachine = now - datetime.timedelta(days=2)
            time_entry_params.update({'from': timemachine})
        else:
            time_entry_params.update({'from': from_date})

        # Prevent pulling running timers
        time_entry_params.update({'is_running': 'false'})

        time_entry_dict = self.__get_api_data__(root_key=root_key, filters=filters,
                                                extra_params=time_entry_params)

        return time_entry_dict

    def get_harvest_users(self):
        root_key = 'users'
        # Name the keys you care about from the api
        filters = ['id', 'first_name', 'last_name', 'email', 'timezone', 'weekly_capacity', 'is_contractor',
                   'is_active', 'roles', 'avatar_url', 'created_at', 'updated_at']

        users_dict = self.__get_api_data__(root_key=root_key, filters=filters)

        return users_dict

    def get_harvest_clients(self):
        root_key = 'clients'
        # Name the keys you care about from the api
        filters = ['id', 'name', 'is_active', 'created_at', 'updated_at']

        clients_dict = self.__get_api_data__(root_key=root_key, filters=filters)

        return clients_dict

    def get_harvest_projects(self):
        root_key = 'projects'
        # Name the keys you care about from the api
        filters = ['id', 'name', 'code', 'is_active', 'is_billable', 'budget', 'budget_is_monthly',
                   'created_at', 'updated_at', 'starts_on', 'ends_on', 'client']

        projects_dict = self.__get_api_data__(root_key=root_key, filters=filters)

        return projects_dict

    def get_harvest_tasks(self):
        root_key = 'tasks'
        # Name the keys you care about from the api
        filters = ['id', 'name']

        tasks_dict = self.__get_api_data__(root_key=root_key, filters=filters)

        return tasks_dict


# Grabs Forecast data Not complete at the moment
class Forecaster(object):

    def __init__(self, forecast_account_id, user_agent, auth_token):
        self.forecast_base_url = 'https://api.forecastapp.com/'
        self.forecast_headers = {}
        self.forecast_headers.update(Authorization=auth_token)
        self.forecast_headers.update({'Forecast-Account-ID': forecast_account_id})
        self.forecast_headers.update({'User-Agent': user_agent})
