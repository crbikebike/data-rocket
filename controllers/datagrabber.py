### File Purpose: This set of classes grabs data from various sources ###

# Imports
import requests, json
from collections import deque
import datetime

# Grabs all Harvest Data based on the "FROM_DATE"
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
                enum_pv = dict(enumerate(pv))
                json_pv = json.dumps(enum_pv)
                flattened_dict.update(roles=json_pv)
            else:
                # If the value isn't a dict, do nothing
                pass

        return flattened_dict

    # Will hit a harvest API and return result as json object
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

        api_json_result.update({root_key: api_list})

        return api_json_result

    def get_harvest_actuals(self, from_date):
        time_entries = []
        api_url = 'time_entries'
        time_entry_params = {}
        time_entry_params.update({'from': from_date})
        time_entry_params.update({'is_running': 'false'})

        # get page numbers, build queue
        actuals_jr = self.__get_request__(api_url=api_url, extra_params=time_entry_params)
        page_qty = actuals_jr['total_pages']
        page_queue = deque(range(1, (page_qty + 1)))

        while len(page_queue) > 0:
            page_num = page_queue.popleft()
            time_entry_params.update(page=page_num)
            page_jr = self.__get_request__(api_url=api_url, extra_params=time_entry_params)

            print('Processing Page: ' + str(page_jr['page']) + ' out of ' + str(page_jr['total_pages']))

            # flatten the time_entries json
            for entry in page_jr['time_entries']:
                time_entry = {}
                time_entry.update(entry_id=entry['id'])
                time_entry.update(user_id=entry['user']['id'])
                time_entry.update(user_name=entry['user']['name'])
                time_entry.update(client_id=entry['client']['id'])
                time_entry.update(client_name=entry['client']['name'])
                time_entry.update(harvest_project_id=entry['project']['id'])
                time_entry.update(harvest_project_name=entry['project']['name'])
                time_entry.update(harvest_project_code=entry['project']['code'])
                time_entry.update(task_id=entry['task']['id'])
                time_entry.update(task_name=entry['task']['name'])
                time_entry.update(billable_rate=entry['billable_rate'])
                time_entry.update(created_at=entry['created_at'])
                time_entry.update(hours=entry['hours'])
                time_entry.update(spent_date=entry['spent_date'])
                time_entry.update(updated_at=entry['updated_at'])
                time_entry.update(billable=entry['billable'])
                if not entry['billable_rate']:
                    time_entry.update(entry_amount=0)
                else:
                    time_entry.update(entry_amount=(entry['hours'] * entry['billable_rate']))

                time_entries.append(time_entry)

        actuals_jr.update(time_entries=time_entries)

        return actuals_jr

    def get_harvest_time_entries(self, from_date):
        # Name the keys you care about from the api
        filters = ['id', 'spent_date', 'hours', 'billable', 'billable_rate', 'created_at', 'updated_at',
                   'user', 'client', 'project', 'task']
        time_entry_params = {}
        if self.is_test:
            now = datetime.datetime.now()
            timemachine = now - datetime.timedelta(days=2)
            time_entry_params.update({'from': timemachine})
        else:
            time_entry_params.update({'from': from_date})
        time_entry_params.update({'is_running': 'false'})

        time_entry_dict = self.__get_api_data__(root_key='time_entries', filters=filters,
                                                extra_params=time_entry_params)

        return time_entry_dict

    # Returns full set of users from Harvest, filtered by the filters list
    def get_harvest_users(self):
        # Name the keys you care about from the api
        filters = ['id', 'first_name', 'last_name', 'email', 'timezone', 'weekly_capacity', 'is_contractor',
                   'is_active', 'roles', 'avatar_url', 'created_at', 'updated_at']

        users_dict = self.__get_api_data__(root_key='users', filters=filters)

        return users_dict


# Grabs Forecast data Not complete at the moment
class Forecaster(object):

    def __init__(self, forecast_account_id, user_agent, auth_token):
        self.forecast_base_url = 'https://api.forecastapp.com/'

        self.forecast_headers = {}
        self.forecast_headers.update(Authorization=auth_token)
        self.forecast_headers.update({'Forecast-Account-ID': forecast_account_id})
        self.forecast_headers.update({'User-Agent': user_agent})
