# Generate Simulated Database

This package is designed to turn raw csv files into a simulated SQL database 
that resembles what the production database might look like. This simulated 
database can then be used as part of the service network created by the root-
level docker-compose.yml file to test neural network training over a network.

## Setup

Create and activate a virtual environment:
```
python -m venv venv
. venv/bin/activate
```
Install requirements:
```
pip install -r requirements.txt
```
Put the csv file you want to create the database from into `datastore/raw_data`.
Note that it must contain the following columns: 
```
'demdonationamts', 'party_affiliation'
```

## Running Create:

Once setup is complete, you should be able to run the create module in gen_db:
```
python -m gen_db.create
```
To customize the database creation functionality, check out the arguments the 
module accepts with:
```
python -m gen_db.create --help
```
