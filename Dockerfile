FROM apache/airflow:slim-latest-python3.13

# 1. System packages → root
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Python deps → airflow
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. dbt project → root (for chown)
USER root
COPY dbt /opt/airflow/dbt
RUN chown -R airflow: /opt/airflow/dbt

# 4. dbt deps → airflow
USER airflow
WORKDIR /opt/airflow/dbt
RUN dbt deps

# 5. Reset workdir
WORKDIR /opt/airflow
