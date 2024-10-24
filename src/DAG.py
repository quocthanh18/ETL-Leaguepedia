from airflow.models import DAG
import os
from airflow.operators.python import PythonOperator
import extractor 
import transformer 
import loader
import creator
from mwrogue.esports_client import EsportsClient
import psycopg2
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


conn = psycopg2.connect(database=Variable.get("ETL_dbname"),
                        user=Variable.get("user"),
                        password=Variable.get("password"),
                        host=Variable.get("host")
                        )
site_dict = {"LoL": EsportsClient("lol"), "s3": S3Hook("s3_conn"), "db_con":conn}

default_args ={
    'owner': 'Quoc Thanh',
    'start_date': '2024-09-26',
    'email_on_failure': True,
    'email': os.getenv("email")
}
dag = DAG(
    dag_id='ETL_Esports_LoL',
    schedule_interval='@yearly',
    default_args=default_args,
)

player_extractor = PythonOperator(
    task_id='player_extractor',
    python_callable=extractor.player_extractor,
    dag=dag,
    op_kwargs=site_dict
)

tournaments_extractor = PythonOperator(
    task_id='tournaments_extractor',
    python_callable=extractor.tournaments_extractor,
    dag=dag,
    op_kwargs=site_dict
)

tournamentresults_extractor = PythonOperator(
    task_id='tournamentresults_extractor',
    python_callable=extractor.tournamentresults_extractor,
    dag=dag,
    op_kwargs=site_dict
)

teams_extractor = PythonOperator(
    task_id='teams_extractor',
    python_callable=extractor.teams_extractor,
    dag=dag,
    op_kwargs=site_dict
)

scoreboardgames_extractor = PythonOperator(
    task_id='scoreboardgames_extractor',
    python_callable=extractor.scoreboardgames_extractor,
    dag=dag,
    op_kwargs=site_dict
)

tournament_transformer = PythonOperator(
    task_id='tournament_transformer',
    python_callable=transformer.tournaments_transformer,
    dag=dag,
    op_kwargs=site_dict
)

players_transformer = PythonOperator(
    task_id='players_transformer',
    python_callable=transformer.players_transformer,
    dag=dag,
    op_kwargs=site_dict
)

tournamentresults_transformer = PythonOperator(
    task_id='tournamentresults_transformer',
    python_callable=transformer.tournamentresults_transformer,
    dag=dag,
    op_kwargs=site_dict
)

scoreboardgames_transformer = PythonOperator(
    task_id='scoreboardgames_transformer',
    python_callable=transformer.scoreboardgames_transformer,
    dag=dag,
    op_kwargs=site_dict
)

teams_transformer = PythonOperator(
    task_id='teams_transformer',
    python_callable=transformer.teams_transformer,
    dag=dag,
    op_kwargs=site_dict
)

tables_creator = PythonOperator(
    task_id='tables_creator',
    python_callable=creator.main,
    dag=dag,
)

teams_loader = PythonOperator(
    task_id='teams_loader',
    python_callable=loader.teams_loader,
    dag=dag,
    op_kwargs=site_dict
)
players_loader = PythonOperator(
    task_id='players_loader',
    python_callable=loader.players_loader,
    dag=dag,
    op_kwargs=site_dict
)

tournaments_loader = PythonOperator(
    task_id='tournaments_loader',
    python_callable=loader.tournaments_loader,
    dag=dag,
    op_kwargs=site_dict
)

tournamentresults_loader = PythonOperator(
    task_id='tournamentresults_loader',
    python_callable=loader.tournamentresults_loader,
    dag=dag,
    op_kwargs=site_dict
)

scoreboardgames_loader = PythonOperator(
    task_id='scoreboardgames_loader',
    python_callable=loader.scoreboardgames_loader,
    dag=dag,
    op_kwargs=site_dict
)


[teams_extractor, tournaments_extractor, tournamentresults_extractor, player_extractor, scoreboardgames_extractor] >> tournament_transformer
tournament_transformer >> [players_transformer, teams_transformer, tournamentresults_transformer, scoreboardgames_transformer] >> tables_creator
tables_creator >> [teams_loader, players_loader, tournaments_loader, scoreboardgames_loader] >> tournamentresults_loader

