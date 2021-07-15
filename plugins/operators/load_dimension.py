from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """Load dimension table from staging tables.

    Args:
        redhift_conn_id (str): Redshift connection id.
        table (str): Table name.
        query (str): SQL query that selects data to be inserted into `table`.
        append (bool): If True, append rows to the existing `table`. 
            If False (default) delete all rows in `table` before inserting 
            the rows returned by `query`.
    """
    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query="",
                 append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.append = append

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append:
            self.log.info(f"Truncating dimension table {self.table}")
            redshift.run(f"TRUNCATE {self.table}")

        self.log.info(f"Inserting data into {self.table} dimension table")
        redshift.run(f"INSERT INTO {self.table} {self.query}")
        