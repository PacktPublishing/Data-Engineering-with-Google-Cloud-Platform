FROM python:3.8.5

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt
ENV AIRFLOW_HOME=/app/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False