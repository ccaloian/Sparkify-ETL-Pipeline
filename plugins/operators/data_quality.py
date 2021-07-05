from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 quality_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.quality_checks = quality_checks
        

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')

        redshift_hook = PostgresHook(self.redshift_conn_id)

        for qc in self.quality_checks:
            records = redshift_hook.get_records(qc["query"])
            result = records[0]
            if result != qc["expected"]:
                raise ValueError(
                    f"Data quality test {qc['name']} failed for table {qc['table']}!"
                    )
            else:
                self.log.info(f"Data quality test {qc['name']} passed for table {qc['table']}!")
