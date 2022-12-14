
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
    'owner': 'fabio_barbazza',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    #'start_date': datetime(1953, 10, 10),
    #'end_date': datetime(2010, 1, 1),
    'email_on_failure': True,
    # retry 1 times after failure
    #'retries': 1,
    'email_on_retry': False,
    # retry after 5 minutes
    #'retry_delay': timedelta(minutes=5),
    'catchup': False
}


with DAG('climate_dag',
          default_args=default_args,
          description='ETL Climate Data'
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
            s3_path = 's3://data-climate/country.csv',
            region= 'us-west-2'
        )

        stage_temperature_country_to_redshift = StageToRedshiftOperator(
            task_id='stage_temperature_country',
            dag=dag,
            redshift_conn_id = "redshift",
            aws_credentials_id="aws_credentials",
            table="temperature_country_stage",
            s3_path = 's3://data-climate/temperature_country.csv',
            region= 'us-west-2',
            type_data_source='csv'
        )


        stage_temperature_state_to_redshift = StageToRedshiftOperator(
            task_id='stage_temperature_state',
            dag=dag,
            redshift_conn_id = "redshift",
            aws_credentials_id="aws_credentials",
            table="temperature_state_stage",
            s3_path = 's3://data-climate/temperature_state.parquet',
            region= 'us-west-2',
            type_data_source='json'
        )


    with TaskGroup(group_id='load_dim_tables') as load_dim_tables:
        # load dimension tables
        load_country_dimension_table = LoadDimensionOperator(
            task_id='load_country_dim_table',
            sql = SqlQueries.insert_country,
            redshift_conn_id = "redshift",
            table = "country"
        )

        # load dimension tables
        load_temperature_country_dimension_table = LoadDimensionOperator(
            task_id='load_temperature_country_dim_table',
            sql = SqlQueries.insert_temperature_country,
            redshift_conn_id = "redshift",
            table = "temperature_country"
        )
        # load dimension tables
        load_temperature_state_dimension_table = LoadDimensionOperator(
            task_id='load_temperature_state_dim_table',
            sql = SqlQueries.insert_temperature_state,
            redshift_conn_id = "redshift",
            table = "temperature_state"
        )


    # create fact table from staging tables
    load_climate_stage = LoadFactOperator(
        task_id='load_climate_stage_table',
        dag=dag,
        sql = SqlQueries.insert_climate_stage,
        redshift_conn_id = "redshift",
        table = "climate_stage"
    )

    load_climate = LoadFactOperator(
        task_id='load_climate_table',
        dag=dag,
        sql = SqlQueries.insert_climate,
        redshift_conn_id = "redshift",
        table = "climate"
    )


    with TaskGroup(group_id='data_quality_check') as data_quality_check:
        # data quality checks
        country_data_quality_checks = DataQualityOperator(
            task_id='country_data_quality_checks',
            table = "country",
            col_name = "country_name",
            redshift_conn_id = "redshift"
        )

        temperature_country_data_quality_checks = DataQualityOperator(
            task_id='temperature_country_data_quality_checks',
            table = "temperature_country",
            col_name = "avg_temperature",
            redshift_conn_id = "redshift"
        )

        temperature_state_data_quality_checks = DataQualityOperator(
            task_id='temperature_state_data_quality_checks',
            table = "temperature_state",
            col_name = "avg_temperature",
            redshift_conn_id = "redshift"
        )

        climate_data_quality_checks = DataQualityOperator(
            task_id='climate_data_quality_checks',
            table = "climate",
            col_name = "avg_temperature",
            redshift_conn_id = "redshift"
        )

    end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



start_operator >> create_table >> load_stage_tables >> load_dim_tables >> load_climate_stage >> load_climate >> data_quality_check >> end_operator