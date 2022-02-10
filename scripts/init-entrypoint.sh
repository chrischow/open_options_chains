#!/usr/bin/env bash
# Wait for Postgres service to be ready
until PGPASSWORD=$POSTGRES_PASSWORD psql -h "postgres" -U "$POSTGRES_USER" -c '\q'; do
  >&2 echo "PostgreSQL service unavailable. Retrying in 5 seconds..."
  sleep 5
done
  
>&2 echo "PostgreSQL service started successfully. Initialising Airflow..."

# Initialise database
airflow db init

# Create account
airflow users create -u "$APP_AIRFLOW_USERNAME" -p "$APP_AIRFLOW_PASSWORD" -f Firstname -l Lastname -r Admin -e admin@airflow.com

# Add connection
airflow connections add 'postgres_optionsdata' \
    --conn-type 'postgres' \
    --conn-login 'openoptions' \
    --conn-password 'openoptions' \
    --conn-host 'postgres' \
    --conn-port '5432' \
    --conn-schema 'optionsdata' \