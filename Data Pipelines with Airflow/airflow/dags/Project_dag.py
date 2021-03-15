from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now(),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past': False
}

dag = DAG('Project_dag',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = "@hourly"
        )

start_operator = DummyOperator(
    task_id = 'Begin_execution',  
    dag = dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    table = "staging_events",
    s3_bucket = Variable.get("s3_bucket"),
    s3_directory = Variable.get("logs_directory"),
    aws_conn = "aws_credentials",
    redshift_conn = "redshift",
    extra_params="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    table = "staging_songs",
    s3_bucket = Variable.get("s3_bucket"),
    s3_directory = Variable.get("songs_directory"),
    aws_conn = "aws_credentials",
    redshift_conn = "redshift",
    extra_params="json 'auto' COMPUPDATE OFF REGION 'us-west-2'"
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    redshift_conn = "redshift",
    table = "songplays",
    sql_source = SqlQueries.songplay_table_insert,
    dag = dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    redshift_conn = "redshift",
    table = "songs",
    sql_source=SqlQueries.song_table_insert,
    dag = dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    redshift_conn = "redshift",
    table = "time",
    sql_source = SqlQueries.time_table_insert,
    dag = dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    redshift_conn = "redshift",
    table = "users",
    sql_source = SqlQueries.user_table_insert,
    dag = dag
)


load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    redshift_conn = "redshift",
    table = "artists",
    sql_source = SqlQueries.artist_table_insert,
    dag = dag
)


# 
#                                                                       -> Load_song_dim_table   ->
#                                                                     /                             \
#                    -> Stage_events                                 /---> Load_user_dim_table   --->\
#                   /               \                               /                                 \
# Begin_execution ->                 -> Load_songplays_fact_table ->                                    -> Run_data_quality_checks -> End_execution
#                   \               /                               \                                 /
#                    -> Stage_songs                                  \---> Load_artist_dim_table --->/
#                                                                     \                             /
#                                                                       -> Load_time_dim_table   ->
#

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table