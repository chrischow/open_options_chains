# Open Options Chains
### *A Data Pipeline Solution for Collecting Options Data at Scale*
This repository contains the code to implement my Airflow/PostgreSQL solution for managing a data pipeline for collection options data. I have written a series of posts detailing the setup:

1. [Database Design](https://medium.datadriveninvestor.com/towards-open-options-chains-a-data-pipeline-for-collecting-options-data-at-scale-part-i-f757c156639b)
2. [Foundational ETL Code](https://medium.datadriveninvestor.com/towards-open-options-chains-part-ii-foundational-etl-code-2bca40251f95)
3. [Getting Started with Airflow](https://medium.datadriveninvestor.com/towards-open-options-chains-part-iii-getting-started-with-airflow-61c7c154f00c)
4. [Building the DAG](https://hackernoon.com/how-to-build-a-directed-acyclic-graph-dag-towards-open-options-chains-part-iv)
5. [Containerising the Pipeline](https://hackernoon.com/towards-open-options-chains-part-v-containerizing-the-pipeline)

## Pre-requisites
1. Operating system: Linux (I'm using Ubuntu 20.04)
2. Docker
3. Docker Compose

## Installation
If you have Git installed, clone the repository to your local machine.

```bash
git clone https://github.com/chrischow/open_options_chains.git
```

Otherwise, you may download the code and unzip the contents in a folder of your choice. Then, `cd` into that directory. You'll see:

- `dags`: Contains the DAGs you'll need for
- `db`: Contains a script to initialise the Postgres database
- `logs`: Contains logs from your Airflow instance
- `pgdata`: Contains data from Postgres
- `scripts`: Contains a script to initialise Airflow
- `docker-compose.yml`: File that defines the services, networks, and volumes for our app

Then, change the required variables in the `.env` file. Note that when you edit the `POSTGRES_*` variables, you must change the associated variables in `AIRFLOW__CORE__SQL_ALCHEMY_CONN`. Do remember to input your TD Ameritrade (TDA) API key.

```yaml
# Postgres
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
APP_DB_USER=openoptions
APP_DB_PASS=openoptions
APP_DB_NAME=optionsdata

# Airflow
APP_AIRFLOW_USERNAME=admin
APP_AIRFLOW_PASSWORD=password
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor

# TDA API
API_KEY=<your TDA API key>
```

## Initialisation
Next, we'll initialise Airflow with the command below. This will:

1. Start a PostgreSQL container
2. Initialise the Airflow Metadata Database and create an Airflow account

```bash
docker-compose up airflow-init
```

The `airflow-init` container will shut down after the initialisation is complete. The `postgres` container will continue running. You can stop it with `docker-compose stop` or `docker stop <container id>` if you don't need it.

## Usage

### Launch
Launch Airflow with the command below. This will:

1. Start a PostgreSQL container if it's not already running
2. Start the Airflow Scheduler container
3. Start the Airflow Webserver container

```bash
docker-compose up
```

In a browser, navigate to [http://localhost:8088](http://localhost:8088) to access the Airflow UI. For the first time login, use the credentials `APP_AIRFLOW_USERNAME` and
`APP_AIRFLOW_PASSWORD` that you set in the `.env` file.

### Shutdown
Use the following command:

```bash
docker-compose stop
```

### Extracting Data

#### Export to CSV
You can extract the data in CSV format via the command line using the commands below:

```bash
# Check Postgres container ID
docker ps

# Go to folder to store files
cd <directory-to-store-csv-files>

# Extract CSV file
docker exec -t <first-few-characters-of-docker-container> psql -U airflow -d optionsdata -c "COPY table_name to STDOUT WITH CSV HEADER" > "filename.csv"
```

#### Connect to Postgres Instance
Alternatively, you can connect to the Postgres database running in the container. Simply use the database name and login credentials set in the `.env` file.

```bash
import psycopg2 as pg2

# Connect to database
conn = pg2.connect(host='localhost', database='optionsdata', user='airflow', password='airflow', port='5432')
```

### Full Reset - Take Caution!
To restart the environment:

```bash
# Completely shut down the app
docker-compose down

# Remove contents of mounted volumes
rm -rf logs/*
rm -rf pgdata/*
```