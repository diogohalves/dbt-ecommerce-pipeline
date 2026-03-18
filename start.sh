#!/bin/bash
set -e
echo "Generating dbt profiles..."
mkdir -p /app/.dbt
cat > /app/.dbt/profiles.yml << PROFILES
ecommerce:
  target: prod
  outputs:
    prod:
      type: postgres
      host: ${DB_HOST}
      port: ${DB_PORT}
      user: ${DB_USER}
      password: ${DB_PASSWORD}
      dbname: ${DB_NAME}
      schema: public
      threads: 1
PROFILES

echo "Running dbt parse to generate semantic manifest..."
cd /app && /app/.venv/bin/dbt parse --profiles-dir /app/.dbt

echo "Starting API..."
cd /app/api && /app/.venv/bin/uvicorn main:app --host 0.0.0.0 --port $PORT
