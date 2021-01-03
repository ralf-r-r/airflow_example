from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 AWS_credentials_id: str,
                 RS_conn_id: str,
                 RS_target_table: str,
                 S3_bucket: str,
                 S3_key: str = '',
                 S3_jsonpath: str = None,
                 S3_region: str = 'us-west-2',

                 *args,
                 **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.AWS_credentials_id = AWS_credentials_id
        self.RS_conn_id = RS_conn_id
        self.RS_target_table = RS_target_table
        self.S3_bucket = S3_bucket
        self.S3_key = S3_key
        self.S3_jsonpath = S3_jsonpath
        self.S3_region = S3_region


    def execute(self, context):
        self.log.info('staging from ' + self.S3_bucket + self.S3_key + ' to ' + self.RS_target_table + ' started')

        redshift = PostgresHook(postgres_conn_id=self.RS_conn_id)
        aws_hook = AwsHook(self.AWS_credentials_id)
        credentials = aws_hook.get_credentials()

        if self.S3_jsonpath != None:
            staging_copy = """
                    COPY {}
                    FROM '{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    REGION AS '{}'
                    JSON '{}'
                    EMPTYASNULL
                    BLANKSASNULL
                    COMPUPDATE OFF
            """

            staging_copy_formatted = staging_copy.format(self.RS_target_table,
                                                         self.S3_bucket + self.S3_key,
                                                         credentials.access_key,
                                                         credentials.secret_key,
                                                         self.S3_region,
                                                         self.S3_jsonpath)

        else:
            staging_copy = """
                    COPY {}
                    FROM '{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    REGION AS '{}'
                    FORMAT AS JSON 'auto' 
                    EMPTYASNULL
                    BLANKSASNULL
                    COMPUPDATE OFF
            """

            staging_copy_formatted = staging_copy.format(self.RS_target_table,
                                                         self.S3_bucket + self.S3_key,
                                                         credentials.access_key,
                                                         credentials.secret_key,
                                                         self.S3_region)

        redshift.run(staging_copy_formatted)

        self.log.info('staging from ' + self.S3_bucket + self.S3_key + ' to ' + self.RS_target_table + ' completed')
