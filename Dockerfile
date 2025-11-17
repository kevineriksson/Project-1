FROM apache/airflow:2.8.3-python3.11

# Copy Airflow dependencies from the ./airflow subfolder
COPY airflow/requirements.txt /tmp/airflow_requirements.txt

# Install core Python dependencies (as the default airflow user)
RUN pip install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r /tmp/airflow_requirements.txt

# Install PyIceberg and its required engines
RUN pip install --no-cache-dir pyiceberg[pyarrow,pandas] fastparquet pyarrow[s3]

# --- Install MinIO Client (mc) for bucket creation ---
# Switch to root to gain permissions for /usr/local/bin
USER root
RUN curl -o /usr/local/bin/mc https://dl.min.io/client/mc/release/linux-amd64/mc \
    && chmod +x /usr/local/bin/mc

# FIX: Install 'libc-bin' to provide the missing 'getent' utility,
# which the 'mc' client requires to resolve configuration directories.
RUN apt-get update && apt-get install -y libc-bin --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user for security
USER airflow