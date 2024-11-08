FROM apache/airflow:2.10.2
ADD requirements.txt .
USER airflow
RUN pip install -r requirements.txt


USER root
RUN mkdir /dbt
RUN apt-get update \
    && apt-get install -y git \
    && apt-get clean
COPY ./leaguepedia /dbt/leaguepedia
RUN chown -R 1000:0 /dbt
