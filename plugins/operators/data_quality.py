from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql_test = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_test = sql_test

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for i in range(len(self.sql_test)):
            records = redshift.get_records(self.sql_test[i]['test_sql'])
        
        if self.sql_test[i]['expected_result'] != records[0][0]:
            raise ValueError(f"Data quality check failed. The query {self.sql_test[i]['test_sql']} \
                             does not result in {self.sql_test[i]['expected_result']}.")
        self.log.info(f"Query {self.sql_test[i]['test_sql']} results in no null values and passed the quality check")