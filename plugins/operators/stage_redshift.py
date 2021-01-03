from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 RS_host: str,
                 RS_dbname: str,
                 RS_user: str,
                 RS_pw: str,
                 RS_port: str,
                 RS_target_table: str,
                 S3_path: str,
                 S3_jsonpath: str,
                 S3_region: str,
                 S3_IAM_role: str,
                 *args,
                 **kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.RS_host = RS_host
        self.RS_dbname = RS_dbname
        self.RS_user = RS_user
        self.RS_pw = RS_pw
        self.RS_port = RS_port
        self.RS_target_table = RS_target_table
        self.S3_path = S3_path
        self.S3_jsonpath = S3_jsonpath
        self.S3_region = S3_region
        self.S3_IAM_role = S3_IAM_role

    def execute(self, context):
        self.log.info('staging from ' + self.S3_path + ' to ' + self.RS_host + ' ' + self.RS_dbname + ' started')

        staging_copy = ("""
        COPY {} FROM {}
        CREDENTIALS 'aws_iam_role={}'
        JSON {}
        EMPTYASNULL
        BLANKSASNULL
        REGION {};
        """).format(self.RS_target_table, self.S3_path, self.S3_IAM_role, self.S3_jsonpath, self.S3_region)

        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
            self.RS_host, self.RS_dbname, self.RS_user, self.RS_pw, self.RS_port
        ))

        cur = conn.cursor()
        cur.execute(staging_copy)
        conn.commit()
        conn.close()

        self.log.info('staging from ' + self.S3_path + ' to ' + self.RS_host + ' ' + self.RS_dbname + ' completed')
