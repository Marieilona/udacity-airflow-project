from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 empty_table="",
                 insert_stmt="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.empty_table = empty_table
        self.insert_stmt = insert_stmt

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Clear data from existing table')
        if self.empty_table == True:
            redshift.run("TRUNCATE TABLE {}".format(self.table))
        
        self.log.info(f'Insert data to table {self.table}')
        redshift.run(getattr(SqlQueries, f"{self.insert_stmt}"))