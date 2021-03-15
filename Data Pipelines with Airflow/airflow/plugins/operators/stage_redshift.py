from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {}
        ;
    """
    
    create_tables = {
        'staging_songs': """
                    CREATE TABLE public.staging_songs (
                        num_songs int4,
                        artist_id varchar(256),
                        artist_name varchar(256),
                        artist_latitude numeric(18,0),
                        artist_longitude numeric(18,0),
                        artist_location varchar(256),
                        song_id varchar(256),
                        title varchar(256),
                        duration numeric(18,0),
                        "year" int4
                    );
        """,
        'staging_events': """
                   CREATE TABLE public.staging_events (
                        artist varchar(256),
                        auth varchar(256),
                        firstname varchar(256),
                        gender varchar(256),
                        iteminsession int4,
                        lastname varchar(256),
                        length numeric(18,0),
                        "level" varchar(256),
                        location varchar(256),
                        "method" varchar(256),
                        page varchar(256),
                        registration numeric(18,0),
                        sessionid int4,
                        song varchar(256),
                        status int4,
                        ts int8,
                        useragent varchar(256),
                        userid int4
                    );

        """
        
    }
    
    #s3_directory = {
    #    'staging_songs': Variable.get("songs_directory"),
    #    'staging_events': Variable.get("logs_directory")
    #}
    
    @apply_defaults
    def __init__(self,
                 redshift_conn="",
                 aws_conn="",
                 table="",
                 s3_bucket="",
                 s3_directory="",
                 s3_key="",
                 region= "us-west-2",
                 data_format = "",
                 extra_params = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn = redshift_conn
        self.aws_conn = aws_conn
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_directory = s3_directory
        self.region = region
        self.data_format = data_format
        self.execution_date = kwargs.get('execution_date')
        self.extra_params = extra_params

    def execute(self, context):
        aws_hook = AwsHook(self.aws_conn)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn)
        
        self.log.info(f"Checking if {self.table} exists")
        
        table_exists = redshift.get_records(f"""
            SELECT EXISTS (
                SELECT * FROM information_schema.tables 
                WHERE  table_schema = 'public'
                AND    table_name   = '{self.table}'
            );
        """)[0][0]
        
        
        if table_exists:
            
            self.log.info("Truncating Redshift table")
            redshift.run(f"DELETE FROM {self.table}")
        
        else:
            self.log.info(f"{self.table} doesn't exist, replacing truncate table with create table")
            redshift.run(f"{StageToRedshiftOperator.create_tables[self.table]}")
        
        self.log.info("Copying data from S3 to Redshift")
        
        s3_path = f"s3://{self.s3_bucket}/{self.s3_directory}"
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.extra_params
        )
        
        redshift.run(formatted_sql)
        
        self.log.info("Data copied correctly!")





