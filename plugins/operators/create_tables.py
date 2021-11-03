
   
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTableOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 # Defining operators parameters (with defaults)
                 table_name='',
                 redshift_conn_id='',
                 sql_command='',
                 *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        # Mapping parameters to values
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.sql_command = sql_command

    def execute(self, context):
        self.log.info(f'Creating {self.table} table in Redshift')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        redshift.run(self.sql_command)