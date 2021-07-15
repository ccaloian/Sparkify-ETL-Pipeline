from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get("AWS_KEY")
# AWS_SECRET = os.environ.get("AWS_SECRET")

# Dag for creating tables
create_tables_dag = DAG(
        "create_tables_dag",
        description="Create tables dag",
        start_date = datetime.utcnow()
)

start_operator_ = DummyOperator(task_id="Begin_execution",  dag=create_tables_dag)

# Create tables
create_stage_events_table = PostgresOperator(
    task_id="Create_stage_events",
    dag=create_tables_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.staging_events_table_create
)

create_stage_songs_table = PostgresOperator(
    task_id="Create_stage_songs",
    dag=create_tables_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.staging_songs_table_create
)

create_artists_table = PostgresOperator(
    task_id="Create_artists",
    dag=create_tables_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.artist_table_create
)

create_songs_table = PostgresOperator(
    task_id="Create_songs",
    dag=create_tables_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.songs_table_create
)

create_time_table = PostgresOperator(
    task_id="Create_time",
    dag=create_tables_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.time_table_create
)

create_users_table = PostgresOperator(
    task_id="Create_users",
    dag=create_tables_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.users_table_create
)

create_songplays_table = PostgresOperator(
    task_id="Create_songplays",
    dag=create_tables_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.songplays_table_create
)

end_operator_ = DummyOperator(task_id="Stop_execution",  dag=create_tables_dag)

start_operator_ >> create_stage_events_table
start_operator_ >> create_stage_songs_table
start_operator_ >> create_artists_table
start_operator_ >> create_songs_table
start_operator_ >> create_users_table
start_operator_ >> create_time_table
start_operator_ >> create_songplays_table

create_stage_events_table >> end_operator_
create_stage_songs_table >> end_operator_
create_artists_table >> end_operator_
create_songs_table >> end_operator_
create_users_table >> end_operator_
create_time_table >> end_operator_
create_songplays_table >> end_operator_



# Dag for ETL
default_args = {
    "owner": "udacity",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email": ["cristian.caloian@gmail.com"],
    "email_on_retry": False,
    "email_on_fialure": False,
    "start_date": datetime(2019, 1, 12),
    "catchup": False
}

dag = DAG("udac_example_dag",
          default_args=default_args,
          description="Load and transform data in Redshift with Airflow",
          schedule_interval="0 * * * *"
)


start_operator = DummyOperator(task_id="Begin_execution",  dag=dag)

# Copy data from S3 to Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    schema="s3://udacity-dend/log_json_path.json",
    region="us-west-2"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A",  #TODO: Remove /A/A/A (used only for testing)
    schema="auto",
    region="us-west-2"
)

# Load fact table
load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    query=SqlQueries.songplay_table_insert
)

# Load dimension tables
load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    query=SqlQueries.user_table_insert,
    append=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    query=SqlQueries.song_table_insert,
    append=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    query=SqlQueries.artist_table_insert,
    append=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    query=SqlQueries.time_table_insert,
    append=False
)

# Data quality checks

def get_qc(table, column):
    """Generate `non_empty` and `no_nulls` quality check queries for `table` and `column`.

    Args:
        table (str): Table name
        column (str): Column name

    Returns:
        List[Dict]: List of dicts with keys `table`, `query` and `expected` outcome.
    """
    non_empty = f"SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM {table}"
    no_nulls = f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL"

    qcs = [
        {"table": table, "query": non_empty, "expected": 1, "name": "non_empty"},
        {"table": table, "query": no_nulls, "expected": 0, "name": f"no_nulls_{column}"}
    ]

    return qcs

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    quality_checks=get_qc("songplays", "playid") + \
                   get_qc("users", "userid") + \
                   get_qc("songs", "songid") + \
                   get_qc("artists", "artistid") + \
                   get_qc("time", "start_time")
)

end_operator = DummyOperator(task_id="Stop_execution",  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
