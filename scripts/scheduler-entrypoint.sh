#!/usr/bin/env bash
# Wait for Postgres service to be ready
until PGPASSWORD=$POSTGRES_PASSWORD psql -h "postgres" -U "$POSTGRES_USER" -c '\q'; do
  >&2 echo "PostgreSQL service unavailable. Retrying in 5 seconds..."
  sleep 5
done
  
>&2 echo "PostgreSQL service started successfully. Launching Airflow Scheduler..."

# Launch scheduler
airflow scheduler