import apache_beam as beam
import psycopg2
import mysql.connector
from apache_beam.options.pipeline_options import PipelineOptions
from psycopg2 import OperationalError
from mysql.connector import Error
import datetime

# Define the database connection parameters for PostgreSQL and MySQL
postgres_config = {
    'host': 'localhost',
    'dbname': 'demo',
    'user': 'postgres',
    'password': '',
    'port': 5432
}

mysql_config = {
    'host': 'localhost',
    'database': 'Demo_Schema',
    'user': 'root',
    'password': '',
    'port': 3306
}

# Assume there's a global variable to track the last time we checked
last_checked_time = datetime.datetime.now() - datetime.timedelta(days=1)  # Start with yesterday

# DoFn that reads data from PostgreSQL based on the last_modified timestamp
class PostgresSource(beam.DoFn):
    def process(self, element):
        try:
            postgres_conn = psycopg2.connect(**postgres_config)
            cursor = postgres_conn.cursor()
            query = "SELECT * FROM public.demo_table WHERE last_modified > %s"
            cursor.execute(query, (last_checked_time,))
            
            for row in cursor.fetchall():
                yield row
        except OperationalError as e:
            print(f'Error connecting to PostgreSQL: {e}')
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'postgres_conn' in locals():
                postgres_conn.close()

class MySQLSink(beam.DoFn):
    def process(self, element):
        try:
            mysql_conn = mysql.connector.connect(**mysql_config)
            cursor = mysql_conn.cursor()
            sql = '''
            INSERT INTO demo_table (id, name, address, last_modified) 
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
            name = VALUES(name), address = VALUES(address), last_modified = VALUES(last_modified)
            '''
            # Assume element includes the last_modified timestamp
            cursor.execute(sql, (element[0], element[1], element[2], element[3]))

            if cursor.lastrowid:
                print(f'Inserted dataset with ID {element[0]}')
            else:
                print(f'Updated dataset with ID {element[0]}')

            mysql_conn.commit()
            
        except Error as e:
            print(f'Error connecting to MySQL: {e}')
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'mysql_conn' in locals():
                mysql_conn.close()

def run_pipeline():
    global last_checked_time
    pipeline_options = PipelineOptions()
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Extract data from PostgreSQL
        data = (
            pipeline
            | 'Start' >> beam.Create([None])  # Dummy element to start the pipeline
            | 'ReadFromPostgres' >> beam.ParDo(PostgresSource())
        )
        
        # Load data into MySQL
        data | 'WriteToMySQL' >> beam.ParDo(MySQLSink())

    # Update the last_checked_time to now after the pipeline run
    last_checked_time = datetime.datetime.now()

if __name__ == '__main__':
    run_pipeline()
