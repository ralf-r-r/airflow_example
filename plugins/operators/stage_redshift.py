from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)

    @apply_defaults
    def __init__(self,
                 aws_credentials_id: str,
                 rs_conn_id: str,
                 rs_target_table: str,
                 s3_bucket: str,
                 s3_key: str = '',
                 s3_jsonpath: str = None,
                 s3_region: str = 'us-west-2',
                 *args,
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.aws_credentials_id = aws_credentials_id
        self.rs_conn_id = rs_conn_id
        self.rs_target_table = rs_target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_jsonpath = s3_jsonpath
        self.s3_region = s3_region

    def execute(self, context):

        bucket_path = self.s3_bucket + self.s3_key.format(**context)

        self.log.info('starting staging from ' + bucket_path + ' to ' + self.rs_target_table)

        redshift = PostgresHook(postgres_conn_id=self.rs_conn_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        if self.s3_jsonpath != None:
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

            staging_copy_formatted = staging_copy.format(self.rs_target_table,
                                                         bucket_path,
                                                         credentials.access_key,
                                                         credentials.secret_key,
                                                         self.s3_region,
                                                         self.s3_jsonpath)

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

            staging_copy_formatted = staging_copy.format(self.rs_target_table,
                                                         bucket_path,
                                                         credentials.access_key,
                                                         credentials.secret_key,
                                                         self.s3_region)

        redshift.run(staging_copy_formatted)

        self.log.info('loading from ' + self.s3_bucket + self.s3_key + ' to ' + self.rs_target_table + ' completed')
