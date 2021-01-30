from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries, data_quality

# - define start date, end date and schedule interval
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 2),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
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
    s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
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
    prior_truncate=False,
    rs_table_name='songplays',
    sql_insert=SqlQueries.songplay_table_insert,
    provide_context=True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    rs_conn_id='redshift',
    prior_truncate=True,
    rs_table_name='users',
    sql_insert=SqlQueries.user_table_insert,
    provide_context=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    rs_conn_id='redshift',
    prior_truncate=True,
    rs_table_name='songs',
    sql_insert=SqlQueries.song_table_insert,
    provide_context=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    rs_conn_id='redshift',
    prior_truncate=True,
    rs_table_name='artists',
    sql_insert=SqlQueries.artist_table_insert,
    provide_context=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    rs_conn_id='redshift',
    prior_truncate=True,
    rs_table_name='"time"',
    sql_insert=SqlQueries.time_table_insert,
    provide_context=True
)

# - define quality check operator
checks = data_quality.create_sql_checks()
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    rs_conn_id='redshift',
    data_quality_checks=checks
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# - define task dependencies
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table,
                         load_song_dimension_table,
                         load_artist_dimension_table,
                         load_time_dimension_table]

[load_user_dimension_table,
 load_song_dimension_table,
 load_artist_dimension_table,
 load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
