from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Defining operators parameters (with defaults)
                 tables=[],
                 redshift_conn_id='',
                 sql_queries_results='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Mapping parameters to values 
        self.table = tables
        self.redshift_conn_id = redshift_conn_id
        self.sql_queries_results = sql_queries_results 

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for query, result_function in self.sql_queries_results.items():
            logging.info(f"Running query check: {query}")
            records = redshift_hook.get_records(query)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {query} returned no results")
            num_records = records[0][0]
            if result_function(num_records) is False:
                raise ValueError(
                    f"Data quality check failed. Query returned {num_records} records."
                )
            logging.info(
                f"Data quality check for query {query} passed with {num_records} records as expected"
            )