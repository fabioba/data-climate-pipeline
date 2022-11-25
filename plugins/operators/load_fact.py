"""
This mdoule contains load fact operations

Author: Fabio Barbazza
Date: Nov, 2022
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    fact_query_raw = """
        INSERT INTO public.{table}
        {sql}
    """

    @apply_defaults
    def __init__(self,
                sql = "",
                table ="",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """
        Execute
        """
        try:
            
            execution_date = context['ds']

            self.log.info('execution_date: {}'.format(execution_date))

            redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

            self.sql = self.sql.format(
                execution_date = execution_date
                )

            self.log.info('self.sql: {}'.format(self.sql))

            fact_query = LoadFactOperator.fact_query_raw.format(
                table=self.table,
                sql=self.sql
                )

            self.log.info('fact_query: {}'.format(fact_query))

            redshift.run(fact_query)

            self.log.info('fact_query: success')


        except Exception as err:
            self.log.exception(err)
            raise err