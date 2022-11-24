"""
This mdoule contains data quality check operations

Author: Fabio Barbazza
Date: Nov, 2022
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                table = "",
                col_name = "",
                 redshift_conn_id = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.table = table
        self.col_name = col_name

    def execute(self, context):
        """
        Execute
        """
        try:

            self.log.info('data quality table: {}'.format(self.table))

            redshift = PostgresHook(postgres_conn_id = self.conn_id)

            records = redshift.get_records(f"SELECT COUNT(*) FROM {self.table} WHERE {self.col_name} is NULL")

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {self.table} returned no results")

            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
            self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")



        except Exception as err:
            self.log.exception(err)
            raise err