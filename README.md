# Data Rocket 2.0


Welcome!  This project aims to automatically push separate data into one data warehouse, powering Business Intelligence initiatives.  Highly focused on Harvest and Forecast to start.

It is a Python3 app and pushes to a PostgreSQL database.

Check out the issues list for a view in what the app needs and submit a PR :)

## Build Setup

0. Make sure you have a Heroku account that has access to the rev-datarocket app.  Contact Chris Race or repo owner for details.

1. Ensure that config variables are set in Heroku or the db won't be able to connect.
(https://devcenter.heroku.com/articles/config-vars)
2. Clone repo
``` bash
$ cd your dir
$ 
```
3. Make changes, add, commit, and deploy them.  Note that if you add a new module it needs to be added to the requirements.txt or the app will fail.
``` bash
$ git commit -am "the rocket power is now over 9000"
$ git push heroku master
```
4. Turn down the web dyno
``` bash
$ heroku ps:scale web=0
```
5. Install Heroku Scheduler (https://elements.heroku.com/addons/scheduler)

   Setup a job to run nightly in the scheduler console.
``` bash
$ python main.py all_tables
```


## File Summaries

### root Directory

**main.py**

The main facilitator.  This file kicks off the data pulls from Harvest and Forecast.


**data_rocket_conf.py**

Config file that pulls environment variables from Heroku for APIs and Database access.

**server.py**

Currently exists only to assist in the Heroku Build process.  Would be great to expand into an admin portal...some day.

**requirements.txt and Procfile**

Setup files to ensure Heroku can deploy

### controllers Directory

**ormobjects.py**

Contains the classes that form database schema. Can be imported to any file that wants to talk to a database

**ormcontroller.py**

Contains the methods and functions that connect to and perform the CRUD functions on the database

**datagrabber.py**

Holds all connectivity to Harvest and (coming soon!) Forecast apis

**datamunger.py**

Performs all data manipulation and transformation needed prior to database insertion

**datapusher.py**

Kicks off the munging tasks in the needed order to avoid foreign key constraints

**datacleanser.py**

Finds deleted records from source data and deletes them from the data warehouse also
