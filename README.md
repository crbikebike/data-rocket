# Data Rocket

> Launching RevUnit's data to the moon (data warehouse)

Welcome!  This project aims to automatically push all of RevUnit's separate data into one data warehouse, powering Business Intelligence initiatives.  Highly focused on Harvest and Forecast to start.

It is a Python3 app and pushes to a PostgreSQL database.

Please consult Chris Race before changing database fields and formatting.  This data powers several dashboards that are dependent on the current schema.

The largest needs right now are enhancing the data pull process, tests, logging, and error handling.

## Build Setup

0. Make sure you have a Heroku account that has access to the rev-datarocket app.  Contact Chris Race or repo owner for details.

1. Ensure that config variables are set in Heroku or the db won't be able to connect.
(https://devcenter.heroku.com/articles/config-vars)
2. Clone repo
``` bash
$ git clone https://github.com/RevUnit/data-rocket/
$ cd your dir
```
3. Make changes, add, commit, and deploy them.  Note that if you add a new module it needs to be added to the requirements.txt or the app will fail.
``` bash
$ git add .
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
$ python harvest_sql.py
```


## File Summaries

### root Directory

**harvest_sql.py**

The main workhorse.  This file pulls data from Harvest (Forecast coming soon!) and inserts it to the PostgreSQL database


**data_rocket_conf.py**

Config file that pulls environment variables from Heroku for APIs and Database access.

**server.py**

Currently exists only to assist in the Heroku Build process.  Would be great to expand into an admin portal...some day.

**requirements.txt and Procfile**

Setup files to ensure Heroku can deploy

### controllers Directory

**dbcontroller.py**

Contains the classes that connect to and perform the CRUD functions on the database

*Primary Usage*
Call the DataActor() class to interact with the database.

**sqlstatements.py**

A set of dictionaries containing SQL statements used by the dbcontroller.py classes.
