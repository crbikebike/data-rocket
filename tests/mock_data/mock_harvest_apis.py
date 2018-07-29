
import json


class MockParent:
    def __init__(self):
        pass

    def filter_results(self, results_dict, filter_list):
        for entity in results_dict:
            # Filter results from larger dictionary by subtracting a set of its keys against a given list
            result_keys = set(entity.keys())
            # If the filter list being passed is the default None type, then set it as an empty set
            if filter_list:
                filter_set = set(filter_list)
            else:
                filter_set = set()
            # Compare the two sets, pop all keys that are not in the filter set
            pop_list = list(result_keys - filter_set)

            for pop_key in pop_list:
                entity.pop(pop_key)

        return results_dict

class MockHarvester(MockParent):

    def get_harvest_time_entries(self, updated_since):
        json_copy = harvest_time_entries
        hte = json.loads(json_copy)
        filters = ['id', 'spent_date', 'hours', 'billable', 'billable_rate', 'created_at', 'updated_at',
                   'user', 'client', 'project', 'task']
        hte['time_entries'] = self.filter_results(hte['time_entries'], filters)
        return hte

    def get_harvest_users(self, updated_since):
        json_copy = harvest_users
        h_peeps = json.loads(json_copy)
        filters = ['id', 'first_name', 'last_name', 'email', 'timezone', 'weekly_capacity', 'is_contractor',
                   'is_active', 'roles', 'avatar_url', 'created_at', 'updated_at']
        h_peeps['users'] = self.filter_results(h_peeps['users'], filters)
        return h_peeps

    def get_harvest_clients(self, updated_since):
        json_copy = harvest_clients
        hc = json.loads(json_copy)
        filters = ['id', 'name', 'is_active', 'created_at', 'updated_at']
        hc['clients'] = self.filter_results(hc['clients'], filters)
        return hc

    def get_harvest_projects(self, updated_since):
        json_copy = harvest_projects
        hp = json.loads(json_copy)
        filters = ['id', 'name', 'code', 'is_active', 'is_billable', 'budget', 'budget_is_monthly',
                   'created_at', 'updated_at', 'starts_on', 'ends_on', 'client']
        hp['projects'] = self.filter_results(hp['projects'], filters)
        return hp

    def get_harvest_tasks(self, updated_since):
        json_copy = harvest_tasks
        ht = json.loads(json_copy)
        filters = ['id', 'name', 'updated_at']
        ht['tasks'] = self.filter_results(ht['tasks'], filters)
        return ht


class MockForecaster(MockParent):

    def get_forecast_projects(self):
        json_copy = forecast_projects
        fp = json.loads(json_copy)
        filters = ['id', 'name', 'code', 'start_date', 'end_date', 'harvest_id', 'client_id',
                   'updated_at', 'archived']
        fp['projects'] = self.filter_results(fp, filters)
        return fp

    def get_forecast_people(self):
        json_copy = forecast_people
        f_peeps = json.loads(json_copy)
        filters = ['id', 'harvest_id', 'first_name', 'last_name', 'email', 'updated_at', 'archived']
        f_peeps['people'] = self.filter_results(f_peeps['people'], filters)
        return f_peeps

    def get_forecast_assignments(self):
        json_copy = forecast_assignments
        fa = json.loads(json_copy)
        filters = ['id', 'start_date', 'end_date', 'allocation', 'updated_at', 'project_id', 'person_id']
        fa['assignments'] = self.filter_results(fa['assignments'], filters)
        return fa

    def get_forecast_clients(self):
        json_copy = forecast_clients
        fc = json.loads(json_copy)
        filters = ['id', 'name', 'harvest_id', 'updated_at', 'archived']
        fc['clients'] = self.filter_results(fc['clients'], filters)
        return fc


"""
Mock Data below
"""

forecast_clients = """
    {
    "clients": [
        {
            "id": 211111,
            "name": "client.net",
            "harvest_id": 6111111,
            "archived": false,
            "updated_at": "2015-12-17T21:10:14.627Z",
            "updated_by_id": null
        },
        {
            "id": 222222,
            "name": "HY Searchers",
            "harvest_id": 6222222,
            "archived": false,
            "updated_at": "2015-12-17T21:10:14.402Z",
            "updated_by_id": null
        },
        {
            "id": 233333,
            "name": "Future Seekers",
            "harvest_id": null,
            "archived": false,
            "updated_at": "2015-12-17T21:10:14.435Z",
            "updated_by_id": null
        }
        ]
    }
    """

forecast_people = """
    {
    "people": [
        {
            "id": 711111,
            "first_name": "John",
            "last_name": "Doe",
            "email": "jd@imatest.net",
            "login": "enabled",
            "admin": true,
            "archived": false,
            "subscribed": true,
            "avatar_url": "",
            "roles": [
                "Full-Time",
                "NV",
                "Growth",
                "Non-Billable",
                "Exec"
            ],
            "updated_at": "2018-07-20T03:22:54.749Z",
            "updated_by_id": null,
            "harvest_id": 1111111,
            "weekly_capacity": null,
            "working_days": {
                "monday": true,
                "tuesday": true,
                "wednesday": true,
                "thursday": true,
                "friday": true
            },
            "color_blind": false
        },
        {
            "id": 722222,
            "first_name": "CJ",
            "last_name": "Designerperson",
            "email": "cdj@imatest.net",
            "login": "disabled",
            "admin": false,
            "archived": true,
            "subscribed": false,
            "avatar_url": "",
            "roles": [
                "NV",
                "Boost",
                "Strategy"
            ],
            "updated_at": "2018-07-12T03:57:11.259Z",
            "updated_by_id": 252917,
            "harvest_id": 2222222,
            "weekly_capacity": null,
            "working_days": {
                "monday": true,
                "tuesday": true,
                "wednesday": true,
                "thursday": true,
                "friday": true
            },
            "color_blind": false
        },
        {
            "id": 733333,
            "first_name": "Forecast",
            "last_name": "Seesthefuture",
            "email": "forecaster@imatest.net",
            "login": "enabled",
            "admin": true,
            "archived": true,
            "subscribed": false,
            "avatar_url": "",
            "roles": [
                "NV",
                "Billable",
                "Marketing"
            ],
            "updated_at": "2017-09-26T19:28:48.071Z",
            "updated_by_id": null,
            "harvest_id": null,
            "weekly_capacity": null,
            "working_days": {
                "monday": true,
                "tuesday": true,
                "wednesday": true,
                "thursday": true,
                "friday": true
            },
            "color_blind": false
        }
        ]
    }
    """

forecast_projects ="""
    {
    "projects": [
        {
            "id": 201111,
            "name": "Super Duper Project",
            "color": "black",
            "code": null,
            "notes": null,
            "start_date": "2015-12-24",
            "end_date": "2018-04-27",
            "harvest_id": 81111111,
            "archived": false,
            "updated_at": "2018-07-12T03:57:11.203Z",
            "updated_by_id": null,
            "client_id": 211111,
            "tags": []
        },
                {
            "id": 202222,
            "name": "Forecast Exclusive",
            "color": "black",
            "code": null,
            "notes": null,
            "start_date": "2015-12-24",
            "end_date": "2018-04-27",
            "harvest_id": null,
            "archived": false,
            "updated_at": "2018-07-12T03:57:11.203Z",
            "updated_by_id": null,
            "client_id": 233333,
            "tags": []
        }
        ]
    }
    """

forecast_assignments = """
    {
    "assignments": [
        {
            "id": 90111111,
            "start_date": "2018-07-16",
            "end_date": "2018-07-20",
            "allocation": 5760,
            "notes": null,
            "updated_at": "2017-12-11T23:20:57.972Z",
            "updated_by_id": null,
            "project_id": 211111,
            "person_id": 711111,
            "placeholder_id": null,
            "repeated_assignment_set_id": null,
            "active_on_days_off": false
        },
        {
            "id": 12014616,
            "start_date": "2018-07-23",
            "end_date": "2018-07-27",
            "allocation": 5760,
            "notes": null,
            "updated_at": "2017-12-11T23:20:57.986Z",
            "updated_by_id": null,
            "project_id": 211111,
            "person_id": 722222,
            "placeholder_id": null,
            "repeated_assignment_set_id": 297866,
            "active_on_days_off": false
        },
        {
            "id": 12014617,
            "start_date": "2018-07-30",
            "end_date": "2018-08-03",
            "allocation": 5760,
            "notes": null,
            "updated_at": "2017-12-11T23:20:58.002Z",
            "updated_by_id": null,
            "project_id": 211111,
            "person_id": 722222,
            "placeholder_id": null,
            "repeated_assignment_set_id": 297866,
            "active_on_days_off": false
        },
        {
            "id": 12014618,
            "start_date": "2018-08-06",
            "end_date": "2018-08-10",
            "allocation": 5760,
            "notes": null,
            "updated_at": "2017-12-11T23:20:58.017Z",
            "updated_by_id": null,
            "project_id": 211111,
            "person_id": 722222,
            "placeholder_id": null,
            "repeated_assignment_set_id": 297866,
            "active_on_days_off": false
        },
        {
            "id": 12014619,
            "start_date": "2018-08-13",
            "end_date": "2018-08-17",
            "allocation": 5760,
            "notes": null,
            "updated_at": "2017-12-11T23:20:58.032Z",
            "updated_by_id": null,
            "project_id": 211111,
            "person_id": 722222,
            "placeholder_id": null,
            "repeated_assignment_set_id": 297866,
            "active_on_days_off": false
        }
        ]
    }
    """

harvest_tasks = """
    {
    "tasks": [
        {
            "id": 11111111,
            "name": "Taskso 1.1",
            "billable_by_default": true,
            "default_hourly_rate": null,
            "is_default": false,
            "is_active": true,
            "created_at": "2018-07-01T21:40:45Z",
            "updated_at": "2018-07-01T21:40:45Z"
        },
        {
            "id": 22222222,
            "name": "Clickbang",
            "billable_by_default": true,
            "default_hourly_rate": null,
            "is_default": false,
            "is_active": true,
            "created_at": "2018-07-01T21:38:14Z",
            "updated_at": "2018-07-01T21:38:14Z"
        },
        {
            "id": 33333333,
            "name": "Questions",
            "billable_by_default": true,
            "default_hourly_rate": null,
            "is_default": false,
            "is_active": true,
            "created_at": "2018-07-01T21:38:14Z",
            "updated_at": "2018-07-01T21:38:14Z"
        },
        {
            "id": 44444444,
            "name": "Boxes",
            "billable_by_default": true,
            "default_hourly_rate": null,
            "is_default": false,
            "is_active": true,
            "created_at": "2018-07-01T21:38:14Z",
            "updated_at": "2018-07-01T21:38:14Z"
        }
    ],
    "per_page": 100,
    "total_pages": 1,
    "total_entries": 4,
    "next_page": null,
    "previous_page": null,
    "page": 1,
    "links": {
        "first": "https://api.harvestapp.com/v2/tasks?page=1&per_page=100&updated_since=2018-06-30T21%3A40%3A45Z",
        "next": null,
        "previous": null,
        "last": "https://api.harvestapp.com/v2/tasks?page=1&per_page=100&updated_since=2018-06-30T21%3A40%3A45Z"
    }
    }
    """

harvest_projects = """
    {
    "projects": [
        {
            "id": 81111111,
            "name": "Super Duper Project",
            "code": "5076",
            "is_active": true,
            "is_billable": true,
            "is_fixed_fee": false,
            "bill_by": "People",
            "budget": null,
            "budget_by": "none",
            "budget_is_monthly": false,
            "notify_when_over_budget": false,
            "over_budget_notification_percentage": 80,
            "show_budget_to_all": false,
            "created_at": "2018-07-11T19:59:35Z",
            "updated_at": "2018-07-11T19:59:35Z",
            "starts_on": null,
            "ends_on": null,
            "over_budget_notification_date": null,
            "notes": "",
            "cost_budget": null,
            "cost_budget_include_expenses": false,
            "hourly_rate": null,
            "fee": null,
            "client": {
                "id": 6111111,
                "name": "client.net",
                "currency": "USD"
            }
        },
        {
            "id": 82222222,
            "name": "Just ok Project",
            "code": "5075",
            "is_active": true,
            "is_billable": true,
            "is_fixed_fee": false,
            "bill_by": "People",
            "budget": null,
            "budget_by": "none",
            "budget_is_monthly": false,
            "notify_when_over_budget": false,
            "over_budget_notification_percentage": 80,
            "show_budget_to_all": false,
            "created_at": "2018-07-04T00:09:44Z",
            "updated_at": "2018-07-04T00:09:44Z",
            "starts_on": null,
            "ends_on": null,
            "over_budget_notification_date": null,
            "notes": "",
            "cost_budget": null,
            "cost_budget_include_expenses": false,
            "hourly_rate": null,
            "fee": null,
            "client": {
                "id": 6222222,
                "name": "HY Searchers",
                "currency": "USD"
            }
        },
        {
            "id": 83333333,
            "name": "Pre-flight",
            "code": "5064",
            "is_active": true,
            "is_billable": true,
            "is_fixed_fee": false,
            "bill_by": "People",
            "budget": null,
            "budget_by": "project_cost",
            "budget_is_monthly": false,
            "notify_when_over_budget": true,
            "over_budget_notification_percentage": 80,
            "show_budget_to_all": false,
            "created_at": "2018-04-26T21:49:59Z",
            "updated_at": "2018-07-02T21:51:46Z",
            "starts_on": "2018-05-07",
            "ends_on": "2018-07-09",
            "over_budget_notification_date": "2018-07-18",
            "notes": "",
            "cost_budget": 115000,
            "cost_budget_include_expenses": false,
            "hourly_rate": null,
            "fee": null,
            "client": {
                "id": 6222222,
                "name": "HY Searchers",
                "currency": "USD"
            }
        },
        {
            "id": 84444444,
            "name": "YUP Team",
            "code": "5057",
            "is_active": true,
            "is_billable": true,
            "is_fixed_fee": false,
            "bill_by": "People",
            "budget": null,
            "budget_by": "project_cost",
            "budget_is_monthly": true,
            "notify_when_over_budget": true,
            "over_budget_notification_percentage": 80,
            "show_budget_to_all": true,
            "created_at": "2018-04-09T18:04:05Z",
            "updated_at": "2018-07-03T20:20:10Z",
            "starts_on": "2018-04-01",
            "ends_on": null,
            "over_budget_notification_date": null,
            "notes": "",
            "cost_budget": 20000,
            "cost_budget_include_expenses": false,
            "hourly_rate": null,
            "fee": null,
            "client": {
                "id": 6333333,
                "name": "PUT",
                "currency": "USD"
            }
        },
        {
            "id": 85555555,
            "name": "Grimmonz App 2018",
            "code": "5077",
            "is_active": true,
            "is_billable": true,
            "is_fixed_fee": false,
            "bill_by": "People",
            "budget": null,
            "budget_by": "project_cost",
            "budget_is_monthly": false,
            "notify_when_over_budget": false,
            "over_budget_notification_percentage": 80,
            "show_budget_to_all": false,
            "created_at": "2018-03-14T15:11:22Z",
            "updated_at": "2018-07-18T18:36:20Z",
            "starts_on": "2018-03-01",
            "ends_on": "2018-12-31",
            "over_budget_notification_date": null,
            "notes": "",
            "cost_budget": 300000,
            "cost_budget_include_expenses": false,
            "hourly_rate": null,
            "fee": null,
            "client": {
                "id": 6333333,
                "name": "PUT",
                "currency": "USD"
            }
        }
    ],
    "per_page": 100,
    "total_pages": 1,
    "total_entries": 5,
    "next_page": null,
    "previous_page": null,
    "page": 1,
    "links": {
        "first": "https://api.harvestapp.com/v2/projects?page=1&per_page=100&updated_since=2018-07-01T00%3A00%3A00Z",
        "next": null,
        "previous": null,
        "last": "https://api.harvestapp.com/v2/projects?page=1&per_page=100&updated_since=2018-07-01T00%3A00%3A00Z"
    }
    }
    """

harvest_clients = """
    {
    "clients": [
        {
            "id": 6111111,
            "name": "client.net",
            "is_active": true,
            "address": null,
            "created_at": "2018-04-26T21:49:59Z",
            "updated_at": "2018-07-09T14:02:13Z",
            "currency": "USD"
        },
        {
            "id": 6222222,
            "name": "HY Searchers",
            "is_active": true,
            "address": null,
            "created_at": "2017-10-31T15:14:05Z",
            "updated_at": "2018-07-09T13:49:20Z",
            "currency": "USD"
        },
        {
            "id": 6333333,
            "name": "PUT",
            "is_active": true,
            "address": null,
            "created_at": "2017-10-02T20:34:07Z",
            "updated_at": "2018-07-09T13:45:32Z",
            "currency": "USD"
        }
    ],
    "per_page": 100,
    "total_pages": 1,
    "total_entries": 11,
    "next_page": null,
    "previous_page": null,
    "page": 1,
    "links": {
        "first": "https://api.harvestapp.com/v2/clients?page=1&per_page=100&updated_since=2018-07-01T00%3A00%3A00Z",
        "next": null,
        "previous": null,
        "last": "https://api.harvestapp.com/v2/clients?page=1&per_page=100&updated_since=2018-07-01T00%3A00%3A00Z"
    }
    }
    """

harvest_users = """{
                        "users": [
                            {
                                "id": 1111111,
                                "first_name": "John",
                                "last_name": "Doe",
                                "email": "john.doe@imatest.net",
                                "telephone": "",
                                "timezone": "Central Time (US & Canada)",
                                "weekly_capacity": 144000,
                                "has_access_to_all_future_projects": false,
                                "is_contractor": false,
                                "is_admin": false,
                                "is_project_manager": false,
                                "can_see_rates": false,
                                "can_create_projects": false,
                                "can_create_invoices": false,
                                "is_active": true,
                                "created_at": "2018-07-05T22:01:48Z",
                                "updated_at": "2018-07-16T17:56:10Z",
                                "default_hourly_rate": null,
                                "cost_rate": null,
                                "roles": [
                                    "Technology"
                                ],
                                "avatar_url": "https://yahoo.com"
                            },
                            {
                                "id": 2222222,
                                "first_name": "CJ",
                                "last_name": "Designerperson",
                                "email": "cjd@imatest.net",
                                "telephone": "",
                                "timezone": "Central Time (US & Canada)",
                                "weekly_capacity": 144000,
                                "has_access_to_all_future_projects": false,
                                "is_contractor": false,
                                "is_admin": false,
                                "is_project_manager": false,
                                "can_see_rates": false,
                                "can_create_projects": false,
                                "can_create_invoices": false,
                                "is_active": true,
                                "created_at": "2018-05-01T20:27:01Z",
                                "updated_at": "2018-07-16T13:30:50Z",
                                "default_hourly_rate": null,
                                "cost_rate": null,
                                "roles": [
                                    "Design (Ops)",
                                    "Pantswearer",
                                    "Contractor"
                                    
                                ],
                                "avatar_url": "https://yahoo.com"
                            },
                            {
                                "id": 3333333,
                                "first_name": "Chris",
                                "last_name": "Technologist",
                                "email": "chris@imatest.net",
                                "telephone": "",
                                "timezone": "Pacific Time (US & Canada)",
                                "weekly_capacity": 144000,
                                "has_access_to_all_future_projects": false,
                                "is_contractor": true,
                                "is_admin": false,
                                "is_project_manager": false,
                                "can_see_rates": false,
                                "can_create_projects": false,
                                "can_create_invoices": false,
                                "is_active": true,
                                "created_at": "2018-01-30T00:42:50Z",
                                "updated_at": "2018-07-17T16:57:28Z",
                                "default_hourly_rate": 100,
                                "cost_rate": 50,
                                "roles": ["Exec",
                                        "Full-Time",
                                        "Growth",
                                        "Non-Billable",
                                        "NV",
                                        "Strategy"
                                ],
                                "avatar_url": ""
                            },
                            {
                                "id": 4444444,
                                "first_name": "Tony",
                                "last_name": "VPDude",
                                "email": "chris@imavp.net",
                                "telephone": "",
                                "timezone": "Pacific Time (US & Canada)",
                                "weekly_capacity": 144000,
                                "has_access_to_all_future_projects": false,
                                "is_contractor": true,
                                "is_admin": false,
                                "is_project_manager": false,
                                "can_see_rates": false,
                                "can_create_projects": false,
                                "can_create_invoices": false,
                                "is_active": true,
                                "created_at": "2018-01-30T00:42:50Z",
                                "updated_at": "2018-07-17T16:57:28Z",
                                "default_hourly_rate": 100,
                                "cost_rate": 50,
                                "roles": [
                                    "Design (Support)",
                                    "Mission Control",
                                    "Product"
                                ],
                                "avatar_url": ""
                            }
                        ],
                        "per_page": 100,
                        "total_pages": 1,
                        "total_entries": 3,
                        "next_page": null,
                        "previous_page": null,
                        "page": 1,
                        "links": {
                            "first": null,
                            "next": null,
                            "previous": null,
                            "last": null
                        }
                    }
                    """


harvest_time_entries = """
                        {
                        "time_entries": [
                                    {
                                        "id": 111111111,
                                        "spent_date": "2018-07-20",
                                        "hours": 8,
                                        "notes": "Yerp",
                                        "is_locked": false,
                                        "locked_reason": null,
                                        "is_closed": false,
                                        "is_billed": false,
                                        "timer_started_at": null,
                                        "started_time": null,
                                        "ended_time": null,
                                        "is_running": false,
                                        "billable": false,
                                        "budgeted": false,
                                        "billable_rate": null,
                                        "cost_rate": null,
                                        "created_at": "2018-07-23T00:57:54Z",
                                        "updated_at": "2018-07-23T00:58:07Z",
                                        "user": {
                                            "id": 1111111,
                                            "name": "John Doe"
                                        },
                                        "client": {
                                            "id": 6111111,
                                            "name": "client.net",
                                            "currency": "USD"
                                        },
                                        "project": {
                                            "id": 81111111,
                                            "name": "Super Duper Project",
                                            "code": "1897"
                                        },
                                        "task": {
                                            "id": 11111111,
                                            "name": "Taskso 1.1"
                                        },
                                        "user_assignment": {
                                            "id": 333333,
                                            "is_project_manager": false,
                                            "is_active": true,
                                            "budget": null,
                                            "created_at": "2014-03-17T18:08:57Z",
                                            "updated_at": "2014-03-17T18:10:20Z",
                                            "hourly_rate": 125
                                        },
                                        "task_assignment": {
                                            "id": 333333,
                                            "billable": false,
                                            "is_active": true,
                                            "created_at": "2014-03-17T18:28:00Z",
                                            "updated_at": "2014-03-17T18:28:43Z",
                                            "hourly_rate": 0,
                                            "budget": null
                                        },
                                        "invoice": null,
                                        "external_reference": null
                                    },
                                    {
                                        "id": 222222222,
                                        "spent_date": "2018-07-29",
                                        "hours": 8,
                                        "notes": "Yerp",
                                        "is_locked": false,
                                        "locked_reason": null,
                                        "is_closed": false,
                                        "is_billed": false,
                                        "timer_started_at": null,
                                        "started_time": null,
                                        "ended_time": null,
                                        "is_running": false,
                                        "billable": false,
                                        "budgeted": false,
                                        "billable_rate": null,
                                        "cost_rate": null,
                                        "created_at": "2018-07-23T00:57:54Z",
                                        "updated_at": "2018-07-23T00:58:07Z",
                                        "user": {
                                            "id": 1111111,
                                            "name": "John Doe"
                                        },
                                        "client": {
                                            "id": 6111111,
                                            "name": "client.net",
                                            "currency": "USD"
                                        },
                                        "project": {
                                            "id": 81111111,
                                            "name": "Super Duper Project",
                                            "code": "1897"
                                        },
                                        "task": {
                                            "id": 11111111,
                                            "name": "Taskso 1.1"
                                        },
                                        "user_assignment": {
                                            "id": 333333,
                                            "is_project_manager": false,
                                            "is_active": true,
                                            "budget": null,
                                            "created_at": "2014-03-17T18:08:57Z",
                                            "updated_at": "2014-03-17T18:10:20Z",
                                            "hourly_rate": 125
                                        },
                                        "task_assignment": {
                                            "id": 333333,
                                            "billable": false,
                                            "is_active": true,
                                            "created_at": "2014-03-17T18:28:00Z",
                                            "updated_at": "2014-03-17T18:28:43Z",
                                            "hourly_rate": 0,
                                            "budget": null
                                        },
                                        "invoice": null,
                                        "external_reference": null
                                    },
                                    {
                                        "id": 333333333,
                                        "spent_date": "2018-07-18",
                                        "hours": 5.75,
                                        "notes": "Mocked up a few screens for the dynamic task name concept ",
                                        "is_locked": false,
                                        "locked_reason": null,
                                        "is_closed": false,
                                        "is_billed": false,
                                        "timer_started_at": null,
                                        "started_time": null,
                                        "ended_time": null,
                                        "is_running": false,
                                        "billable": true,
                                        "budgeted": true,
                                        "billable_rate": 167,
                                        "cost_rate": null,
                                        "created_at": "2018-07-23T01:00:29Z",
                                        "updated_at": "2018-07-23T01:01:37Z",
                                         "client": {
                                            "id": 6111111,
                                            "name": "client.net",
                                            "currency": "USD"
                                        },
                                        "project": {
                                            "id": 81111111,
                                            "name": "Super Duper Project",
                                            "code": "1897"
                                        },
                                        "task": {
                                            "id": 11111111,
                                            "name": "Taskso 1.1"
                                        },
                                        "user_assignment": {
                                            "id": 146845277,
                                            "is_project_manager": false,
                                            "is_active": true,
                                            "budget": null,
                                            "created_at": "2018-02-06T15:25:38Z",
                                            "updated_at": "2018-02-06T15:25:38Z",
                                            "hourly_rate": 167
                                        },
                                        "task_assignment": {
                                            "id": 177513241,
                                            "billable": true,
                                            "is_active": true,
                                            "created_at": "2018-02-06T15:25:38Z",
                                            "updated_at": "2018-02-06T15:25:38Z",
                                            "hourly_rate": null,
                                            "budget": null
                                        },
                                        "invoice": null,
                                        "external_reference": null
                                    }
                            ],
                            "per_page": 100,
                            "total_pages": 1,
                            "total_entries": 3,
                            "next_page": null,
                            "previous_page": null,
                            "page": 1,
                            "links": {
                                "first": "",
                                "next": null,
                                "previous": null,
                                "last": ""
                                }
                        }
                        """
