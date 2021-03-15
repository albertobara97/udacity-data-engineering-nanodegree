from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (checkAwsAccount, checkRedshiftConnection)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'me',
    'start_date': datetime.now(),
    'email_on_retry': False,
    'catchup': False
}

dag = DAG('only_dummies',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow'
        )

start_operator = DummyOperator(
    task_id='Begin_execution',  
    dag=dag
)

middle_operator = DummyOperator(
    task_id='Middle_operator',
    dag=dag
)

end_operator = DummyOperator(
    task_id='End_execution',  
    dag=dag
)

                                                             

start_operator >> middle_operator

middle_operator >> end_operator
