"""Airflow Operator to load JSON data from S3 into the AWS Redshift staging table"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Operator to load data from Amazon S3 into Amazon Redshift

    """
    ui_color = '#358140'

    COPY_SQL = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS {}
        """

    TRUNCATE_SQL = """
        TRUNCATE TABLE {};
        """

    template_fields = ['s3_path']

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 aws_credentials_id='aws_credentials',
                 table_name='',
                 s3_path='',
                 copy_format='',
                 truncate_table=False,
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.s3_path = s3_path
        self.aws_credentials_id = aws_credentials_id
        self.copy_format = copy_format
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info(f"S3 path: {self.s3_path}")

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.truncate_table:
            # Truncate table
            self.log.info(f"Truncating table {self.table_name}")
            redshift_hook.run(self.TRUNCATE_SQL.format(self.table_name))

        sql_stmt = self.COPY_SQL.format(
            self.table_name,
            self.s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.copy_format
        )

        self.log.info(f"Copying data to staging table: {self.table_name}")
        redshift_hook.run(sql_stmt)
        self.log.info(f"Copying completed for table: {self.table_name}")
