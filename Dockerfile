FROM apache/airflow:2.8.3-python3.11

# Copy Airflow dependencies from the ./airflow subfolder
COPY airflow/requirements.txt /tmp/airflow_requirements.txt

# Install Python dependencies, including dbt-core and dbt-clickhouse
RUN pip install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r /tmp/airflow_requirements.txt