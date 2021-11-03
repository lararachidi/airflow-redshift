from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, CreateTableOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

### Creating the DAG that performs the following: 
###   1. Creates tables 
###   2. Stages events and songs data from S3 to Redshift
###   3. Loads data into the fact and dimensions tables 
###   4. Runs data quality checks 

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False, 
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

### Start Operator 
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

### Creating tables Operators 
create_staging_events = CreateTableOperator(
    task_id='create_staging_events',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='staging_events',
    sql_command=SqlQueries.create_staging_events_table
)

create_staging_songs = CreateTableOperator(
    task_id='create_staging_songs',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='staging_songs',
    sql_command=SqlQueries.create_staging_songs_table
)

create_songplays = CreateTableOperator(
    task_id='create_songplays',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='songplays',
    sql_command=SqlQueries.create_songplays_table
)
    
create_artists = CreateTableOperator(
    task_id='create_artists',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='artists',
    sql_command=SqlQueries.create_artists_table
)

create_songs = CreateTableOperator(
    task_id='create_songs',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='songs',
    sql_command=SqlQueries.create_songs_table
)

create_time = CreateTableOperator(
    task_id='create_time',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='time',
    sql_command=SqlQueries.create_time_table
)

create_users = CreateTableOperator(
    task_id='create_users',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='users',
    sql_command=SqlQueries.create_users_table
)


### Staging events data from S3 to Redshift Operator
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    target_table='staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data/{execution_date.year}/{execution_date.month}',
    s3_format='json',
    s3_format_mode='s3://udacity-dend/log_json_path.json',
    region='us-west-2',
    render_s3_key=True,
    truncate=True
)

### Staging songs data from S3 to Redshift Operator
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    target_table='staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    s3_format='json',
    s3_format_mode='auto',
    region='us-west-2',
    render_s3_key=False,
    truncate=True
)

### Loading data into the fact table Operator
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    target_table='songplays',
    target_columns='(playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)',
    redshift_conn_id='redshift',
    sql_transform_command=SqlQueries.songplay_table_insert,
    truncate=False
)

### Loading data into the four dimension tables Operator
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    target_table='users',
    target_columns='(userid, first_name, last_name, gender, level)',
    redshift_conn_id='redshift',
    sql_transform_command=SqlQueries.user_table_insert,
    truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    target_table='songs',
    target_columns='(songid, title, artistid, year, duration)',
    redshift_conn_id='redshift',
    sql_transform_command=SqlQueries.song_table_insert,
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    target_table='artists',
    target_columns='(artistid, name, location, lattitude, longitude)',
    redshift_conn_id='redshift',
    sql_transform_command=SqlQueries.artist_table_insert,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    target_table='time',
    target_columns='(start_time, hour, day, week, month, year, weekday)',
    redshift_conn_id='redshift',
    sql_transform_command=SqlQueries.time_table_insert,
    truncate=True
)

### Running data quality checks 
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    tables = ["artists", "songplays", "songs"]
)

### End Operator 
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

### Dependencies 
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator 


