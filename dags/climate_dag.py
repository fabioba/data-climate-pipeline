
"""
This mdoule contains the DAG responsible for running the ETL of climate dag

Author: Fabio Barbazza
Date: Nov, 2022
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.postgres_operator import PostgresOperator


from operators.stage_redshift import StageToRedshiftOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.data_quality import DataQualityOperator

from helpers import SqlQueries

import logging 
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

logger = logging.getLogger(__name__)

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(1990, 1, 12),
    'email_on_failure': True,
    # retry 3 times after failure
    'retries': 3,
    'email_on_retry': False,
    # retry after 5 minutes
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'catchup': False
}

with DAG('climate_dag',
          default_args=default_args,
          description='ETL Climate Data',
          schedule_interval='0 * * * *'
        ) as dag:

    start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


    create_table = PostgresOperator(
        task_id="create_table",
        dag=dag,
        postgres_conn_id="redshift",
        sql = 'sql_queries/create_tables.sql'
    )    

    # copy data from S3 to redshift
    stage_temperature_to_redshift = StageToRedshiftOperator(
        task_id='stage_temperature',
        dag=dag,
        redshift_conn_id = "redshift",
        aws_credentials_id="aws_credentials",
        table="temperature_stage",
        s3_path = 's3a://data-climate/temperature.csv',
        region= 'us-west-2'
    )

    stage_air_pollution_to_redshift = StageToRedshiftOperator(
        task_id='stage_air_pollution',
        dag=dag,
        redshift_conn_id = "redshift",
        aws_credentials_id="aws_credentials",
        table="air_pollution_stage",
        s3_path = 's3a://data-climate/air_pollution.csv',
        region= 'us-west-2'
    )

    stage_population_to_redshift = StageToRedshiftOperator(
        task_id='stage_population',
        dag=dag,
        redshift_conn_id = "redshift",
        aws_credentials_id="aws_credentials",
        table="population_stage",
        s3_path = 's3a://data-climate/population.csv',
        region= 'us-west-2'
    )

    # create fact table from staging tables
    load_songplays_table = LoadFactOperator(
        task_id='load_songplays_fact_table',
        dag=dag,
        table = "songplays",
        sql = SqlQueries.songplay_table_insert,
        redshift_conn_id = "redshift"
    )


    with TaskGroup(group_id='load_dimension_tables') as load_dimension_tables:
        # load dimension tables
        load_user_dimension_table = LoadDimensionOperator(
            task_id='load_user_dim_table',
            table = "users",
            sql = SqlQueries.user_table_insert,
            redshift_conn_id = "redshift"
        )

        load_song_dimension_table = LoadDimensionOperator(
            task_id='load_song_dim_table',
            table = "songs",
            sql = SqlQueries.song_table_insert,
            redshift_conn_id = "redshift"
        )

        load_artist_dimension_table = LoadDimensionOperator(
            task_id='load_artist_dim_table',
            table = "artists",
            sql = SqlQueries.artist_table_insert,
            redshift_conn_id = "redshift"
        )

        load_time_dimension_table = LoadDimensionOperator(
            task_id='load_time_dim_table',
            table = "time",
            sql = SqlQueries.time_table_insert,
            redshift_conn_id = "redshift"
        )



    with TaskGroup(group_id='data_quality_check') as data_quality_check:
        # data quality checks
        artists_data_quality_checks = DataQualityOperator(
            task_id='artists_data_quality_checks',
            table = "artists",
            col_name = "artist_id",
            redshift_conn_id = "redshift"
        )

        songplays_data_quality_checks = DataQualityOperator(
            task_id='songplays_data_quality_checks',
            table = "songplays",
            col_name = "playid",
            redshift_conn_id = "redshift"
        )

        songs_data_quality_checks = DataQualityOperator(
            task_id='songs_data_quality_checks',
            table = "songs",
            col_name = "songid",
            redshift_conn_id = "redshift"
        )


        time_data_quality_checks = DataQualityOperator(
            task_id='time_data_quality_checks',
            table = "time",
            col_name = "start_time",
            redshift_conn_id = "redshift"
        )


        users_data_quality_checks = DataQualityOperator(
            task_id='users_data_quality_checks',
            table = "users",
            col_name = "userid",
            redshift_conn_id = "redshift")



    end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> load_dimension_tables >> data_quality_check >> end_operator
