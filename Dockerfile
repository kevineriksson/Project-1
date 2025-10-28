# Dockerfile in the PROJECT-1/ root directory
FROM apache/airflow:2.8.1

# Copy Airflow dependencies from the ./airflow subfolder
COPY airflow/requirements.txt /tmp/airflow_requirements.txt

# Install the necessary Python packages for your DAGs
RUN pip install --no-cache-dir -r /tmp/airflow_requirements.txt