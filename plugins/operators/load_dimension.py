from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql_statement='',
                 target_table='',
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.target_table = target_table
        self.truncate = truncate

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        truncate_statement = f"TRUNCATE TABLE {self.target_table};" if self.truncate else ""

        formatted_sql = LoadDimensionOperator.dim_sql_template.format(
            TRUNCATE_STATEMENT=truncate_statement,
            TABLE=self.target_table,
            SELECT_STATEMENT=self.sql_statement,
        )
        self.log.info("Loading dimension table into Redshift...")
        redshift_hook.run(formatted_sql)
        self.log.info("Finished dimension table into Redshift")
