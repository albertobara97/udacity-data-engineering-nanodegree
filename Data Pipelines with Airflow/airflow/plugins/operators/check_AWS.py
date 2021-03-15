from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class checkAwsAccount(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_conn = "",
                 *args, **kwargs):

        super(checkAwsAccount, self).__init__(*args, **kwargs)
        self.aws_conn = aws_conn

    def execute(self, context):
        aws_hook = AwsHook(self.aws_conn)
        self.log.info(f"Testing connectivity with AWS...")
        aws_hook.get_credentials()
        self.log.info(f"Test succedeed!")





