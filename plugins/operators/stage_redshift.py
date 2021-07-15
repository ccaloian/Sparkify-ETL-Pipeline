from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """Load data from S3 to Redshift.

    Args:
        redshift_conn_id (str): Redshift connection id.
        aws_credentials_id (str): AWS credentials id.
        table (str): Table name.
        s3_bucket (str): S3 bucket name.
        s3_key (str): S3 key name for `s3_bucket`.
        schema (str): JSON schema, 'auto' or path to json schema file.
        region (str): AWS region name.
    """
    ui_color = "#358140"

    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        REGION '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 schema="",
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.schema = schema
        self.region = region
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        self.log.info("StageToRedshiftOperator not implemented yet")

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.schema,
            self.region
        )

        self.log.info("Copying data from S3 to Redshift.")
        redshift.run(formatted_sql)
