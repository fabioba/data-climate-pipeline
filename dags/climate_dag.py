
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

from operators.helpers.sql_queries import SqlQueries

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

    with TaskGroup(group_id='load_stage_tables') as load_stage_tables:
        # copy data from S3 to redshift
        stage_country_to_redshift = StageToRedshiftOperator(
            task_id='stage_country',
            dag=dag,
            redshift_conn_id = "redshift",
            aws_credentials_id="aws_credentials",
            table="country_stage",
            s3_path = 's3a://data-climate/country.csv',
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

    # load dimension tables
    load_country_dimension_table = LoadDimensionOperator(
        task_id='load_country_dim_table',
        sql = SqlQueries.country_table_insert,
        redshift_conn_id = "redshift"
    )


    # create fact table from staging tables
    load_air_pollution = LoadFactOperator(
        task_id='load_air_pollution_table',
        dag=dag,
        sql = SqlQueries.insert_air_pollution,
        redshift_conn_id = "redshift"
    )

    with TaskGroup(group_id='data_quality_check') as data_quality_check:
        # data quality checks
        country_data_quality_checks = DataQualityOperator(
            task_id='country_data_quality_checks',
            table = "country",
            col_name = "country_name",
            redshift_conn_id = "redshift"
        )

        air_pollution_data_quality_checks = DataQualityOperator(
            task_id='air_pollution_data_quality_checks',
            table = "air_pollution",
            col_name = "air_pollution_index",
            redshift_conn_id = "redshift"
        )

    end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_table >> load_stage_tables >> load_country_dimension_table >> load_air_pollution >> data_quality_check >> end_operator
