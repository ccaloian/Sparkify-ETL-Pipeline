from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """Load fact table from staging tables.

    Args:
        redhift_conn_id (str): Redshift connection id.
        table (str): Table name.
        query (str): SQL query that selects data to be inserted into `table`.
    """

    ui_color = "#F98866"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Inserting data into {self.table} fact table")
        redshift.run(f"INSERT INTO {self.table} {self.query}")
