### File Purpose: This set of classes grabs data from various sources ###

# Imports
import requests, json
from collections import deque

# Grabs all Harvest Data based on the "FROM_DATE"
class Harvester(object):

    def __init__(self, auth_token, harvest_account_id, user_agent, from_date, is_test=False):
        self.entry_per_page = 100
        self.harvest_base_url = 'https://api.harvestapp.com/v2/'

        self.harvest_headers = {}
        self.harvest_headers.update(Authorization = auth_token)
        self.harvest_headers.update({'Harvest-Account-ID': harvest_account_id})
        self.harvest_headers.update({'User-Agent': user_agent})

        self.harvest_params = {}
        self.harvest_params.update(page_per=self.entry_per_page)
        self.harvest_params.update({'from': from_date})
        self.harvest_params.update({'is_running': 'false'})


    def harvest_actuals(self):
        time_entries = []
        api_url = 'time_entries'
        full_url = self.harvest_base_url + api_url
        headers = self.harvest_headers
        params = self.harvest_params

        # get page numbers, build queue
        r = requests.get(url=full_url, headers=headers, params=params)
        actuals_jr = json.loads(r.text)
        page_qty = actuals_jr['total_pages']
        page_queue = deque(range(1, (page_qty + 1)))

        while len(page_queue) > 0:
            page_num = page_queue.popleft()
            params.update(page=page_num)
            r = requests.get(url=full_url, headers=headers, params=params)

            page_jr = json.loads(r.text)

            print('Processing Page: ' + str(page_jr['page']) + ' out of ' + str(page_jr['total_pages']))

            # flatten the actuals json
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


# Grabs Forecast data Not complete at the moment
class Forecaster(object):

    def __init__(self, forecast_account_id, user_agent, auth_token):
        self.forecast_base_url = 'https://api.forecastapp.com/'


        self.forecast_headers = {}
        self.forecast_headers.update(Authorization = auth_token)
        self.forecast_headers.update({'Forecast-Account-ID': forecast_account_id})
        self.forecast_headers.update({'User-Agent': user_agent})
