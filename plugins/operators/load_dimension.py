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
    dim_query_raw = """
        INSERT INTO public.{table}
        {sql}
    """


    @apply_defaults
    def __init__(self,
                sql = "",
                table ="",
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

            dim_query = LoadDimensionOperator.dim_query_raw.format(
                table=self.table,
                sql=self.sql
                )


            self.log.info('dim_query: {} running'.format(dim_query))

            redshift.run(dim_query)

            self.log.info('dim_query: success')

        except Exception as err:
            self.log.exception(err)
            raise err
