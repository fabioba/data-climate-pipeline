"""
This mdoule contains operations on AWS - Redshift

Author: Fabio Barbazza
Date: Nov, 2022
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    This class perform operations on AWS - Redshift.
    The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. 
    The operator creates and runs a SQL COPY statement based on the parameters provided. 
    The operator's parameters should specify where in S3 the file is loaded and what is the target table.
    """
    ui_color = '#358140'
    copy_sql_from_csv = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                IGNOREHEADER 1
                DELIMITER ';'
                """
    copy_sql_from_json="""
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                JSON '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_path = "",
                 region = "",
                 type_data_source="csv",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_path = s3_path
        self.region = region
        self.type_data_source = type_data_source

    def execute(self, context):
        """
        Execute
        """
        try:

            self.log.info('connect to aws')
            aws_hook = AwsHook(self.aws_credentials_id)
            credentials = aws_hook.get_credentials()

            self.log.info('connect to redshift')
            redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

            self.log.info('remove tables')
            redshift.run("DELETE FROM {}".format(self.table))

            self.log.info('s3_path: {}'.format(self.s3_path))

            if self.type_data_source == 'csv':
                formatted_sql = StageToRedshiftOperator.copy_sql_from_csv.format(
                    self.table,
                    self.s3_path,
                    credentials.access_key,
                    credentials.secret_key
                )

            elif self.type_data_source== 'json':
                formatted_sql = StageToRedshiftOperator.copy_sql_from_json.format(
                    self.table,
                    self.s3_path,
                    credentials.access_key,
                    credentials.secret_key,
                    self.s3_path
                )

            self.log.info('formatted_sql: {}'.format(formatted_sql))
            
            redshift.run(formatted_sql)


        except Exception as err:
            self.log.exception(err)
            raise err





