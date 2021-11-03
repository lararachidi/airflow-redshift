from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql_statement='',
                 target_table='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.target_table = target_table

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        formatted_sql = LoadFactOperator.facts_sql_template.format(
            TABLE=self.target_table, SELECT_STATEMENT=self.sql_statement
        )
        self.log.info("Loading fact table into Redshift...")
        redshift_hook.run(formatted_sql)
        self.log.info("Finished loading fact table into Redshift")