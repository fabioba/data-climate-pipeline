"""
This mdoule contains loading operations

Author: Fabio Barbazza
Date: Nov, 2022
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    dim_query = """
        INSERT INTO public.{table}
        {sql}
    """


    @apply_defaults
    def __init__(self,
                table = "",
                sql = "",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.table = table
        self.conn_id = redshift_conn_id

    def execute(self, context):
        """
        Execute
        """
        try:

            redshift = PostgresHook(postgres_conn_id = self.conn_id)

            self.log.info('self.dim_query: {} running'.format(self.dim_query))

            redshift.run(self.dim_query)

            self.log.info('self.dim_query: success')

        except Exception as err:
            self.log.exception(err)
            raise err
