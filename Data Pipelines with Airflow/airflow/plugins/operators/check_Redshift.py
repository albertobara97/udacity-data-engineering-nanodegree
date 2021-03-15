from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class checkRedshiftConnection(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_conn = "",
                 redshift_conn = "",
                 *args, **kwargs):

        super(checkRedshiftConnection, self).__init__(*args, **kwargs)
        self.aws_conn = aws_conn
        self.redshift_conn = redshift_conn

    def execute(self, context):
        aws_hook = AwsHook(self.aws_conn)
        self.log.info(f"Testing connectivity with Redshift...")
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn)
        
        self.log.info("Creating Redshift testing table")
        
        redshift.run("""
            CREATE TABLE IF NOT EXISTS testing_table(
                id int null
            );
            """)
        
        self.log.info("Table created correctly, deleting testing table...")
        
        redshift.run("DROP TABLE IF EXISTS testing_table;")
        
        self.log.info("Table deleted!")
        




