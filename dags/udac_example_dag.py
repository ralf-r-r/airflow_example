from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries

# - define start date, end date and schedule interval
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 1, 1),
    'end_date': datetime(2018, 10, 1)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 * * *'
          )

# - define start_operator
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# - define staging operators
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials_id='aws_credentials',
    rs_conn_id='redshift',
    rs_target_table='public.staging_events',
    s3_bucket='s3://udacity-dend/',
    s3_key="log_data/{{ execution_date.year }}/{{ execution_date.month }}/{{ ds }}-events.json",
    s3_jsonpath='s3://udacity-dend/log_json_path.json',
    s3_region='us-west-2',
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials_id='aws_credentials',
    rs_conn_id='redshift',
    rs_target_table='public.staging_songs',
    s3_bucket='s3://udacity-dend/',
    s3_key='song_data/',
    s3_jsonpath=None,
    s3_region='us-west-2',
    provide_context=True
)

# - define fact and dimension operators
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    rs_conn_id='redshift',
    prior_truncate=True,
    rs_table_name='songplays',
    sql_insert=SqlQueries.songplay_table_insert,
    provide_context=True
)

"""
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

# - define quality check operator
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)
"""
end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# - define task dependencies
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> end_operator
