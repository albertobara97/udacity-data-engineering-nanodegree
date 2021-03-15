from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (checkAwsAccount, checkRedshiftConnection)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now(),
    'email_on_retry': False,
    'catchup': False,
    'depends_on_past': False
}

dag = DAG('check_hook_connections_v1',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow'
        )

start_operator = DummyOperator(
    task_id = 'Begin_execution',  
    dag = dag
)

check_AWS_account = checkAwsAccount(
    task_id = 'check_AWS_account',
    dag = dag,
    aws_conn = "aws_credentials"
)

check_Redshift_connection = checkRedshiftConnection(
    task_id = 'check_Redshift_connection',
    dag = dag,
    aws_conn = "aws_credentials",
    redshift_conn = "redshift"
)

end_operator = DummyOperator(
    task_id='End_execution',  
    dag=dag
)


# 
#                                                                   
#                                                              
#                    -> check_AWS_account --------->                             
#                   /                               \                              
# Begin_execution ->                                 -> End_execution 
#                   \                               /                              
#                    -> check_Redshift_connection ->                               
#                                                                   
#                                                                    
#

start_operator >> check_AWS_account
start_operator >> check_Redshift_connection

check_AWS_account >> end_operator
check_Redshift_connection >> end_operator
