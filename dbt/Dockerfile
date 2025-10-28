# dbt/Dockerfile
FROM python:3.11-slim

# Copy the specific dbt requirements file from the root
# We are assuming requirements.txt for dbt is in the root directory
COPY requirements.txt /tmp/dbt_requirements.txt

# Install dbt-core, dbt-clickhouse, and clickhouse-connect
RUN pip install --no-cache-dir -r /tmp/dbt_requirements.txt

# Set a working directory
WORKDIR /usr/app