from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CustomDummyOperator(BaseOperator):
    """
    Operator that does literally nothing. It can be used to group tasks in a
    DAG.

    The task is evaluated by the scheduler but never processed by the executor.
    """

    ui_color = '#e8f7e4'


    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DummyOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        pass





