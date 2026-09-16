# FROM apache/airflow:2.9.2
# COPY requirements.txt /
# RUN pip install --no-cache-dir -r /requirements.txt
FROM apache/airflow:3.3.1-python3.12
COPY python-packages /python-packages
RUN pip install --no-index --find-links=/python-packages apache-airflow-providers-amazon apache-airflow-providers-postgres apache-airflow-providers-elasticsearch requests
