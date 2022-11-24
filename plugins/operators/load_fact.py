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


    @apply_defaults
    def __init__(self,
                sql = "",
                table = "",
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

            redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

            self.log.info('self.sql: {}'.format(self.sql))

            redshift.run(self.sql)

            self.log.info('self.sql: success')


        except Exception as err:
            self.log.exception(err)
            raise err