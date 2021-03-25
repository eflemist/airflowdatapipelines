from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_sql = """
        INSERT INTO {}
        {}
     """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_insert="",    
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_insert = sql_insert

    def execute(self, context):
        self.log.info('LoadFactOperator started...')
        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift_hook.run(LoadFactOperator.insert_sql.format(self.table,self.sql_insert))

