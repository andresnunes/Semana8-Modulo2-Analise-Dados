FROM apache/airflow:3.3.2

COPY requirements.txt /tmp/requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt
