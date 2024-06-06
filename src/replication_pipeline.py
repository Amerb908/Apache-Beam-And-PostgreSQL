#This code sets up an Apache Beam pipeline to replicate data from a PostgreSQL database to a MySQL database, 
# handling UPSERT operations to maintain data integrity.
import apache_beam as beam
import psycopg2
import mysql.connector
from apache_beam.options.pipeline_options import PipelineOptions
from psycopg2 import OperationalError
from mysql.connector import Error

# Database configuration for PostgreSQL and MySQL
postgres_config = {
    'host': 'localhost',
    'dbname': 'demo',
    'user': 'postgres',
    'password': 'Momnoor9696@!',
    'port': 5432
}

mysql_config = {
    'host': 'localhost',
    'database': 'Demo_Schema',
    'user': 'root',
    'password': '',
    'port': 3306
}

# DoFn that reads data from PostgreSQL
class PostgresSource(beam.DoFn):
    def process(self, element):
        try:
            with psycopg2.connect(**postgres_config) as conn:
                with conn.cursor() as cursor:
                    cursor.execute('SELECT * FROM public.demo_table')
                    for row in cursor.fetchall():
                        yield row
        except OperationalError as e:
            print(f'Error connecting to PostgreSQL: {e}')

# DoFn for UPSERT operations in MySQL
class MySQLSink(beam.DoFn):
    def process(self, element):
        try:
            with mysql.connector.connect(**mysql_config) as conn:
                with conn.cursor() as cursor:
                    sql = '''
                    INSERT INTO demo_table (id, name, address) 
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                    name = VALUES(name), address = VALUES(address)
                    '''
                    cursor.execute(sql, element)
                    conn.commit()
                    print(f'Processed dataset with ID {element[0]}')
        except Error as e:
            print(f'Error connecting to MySQL: {e}')

def run_pipeline():
    options = PipelineOptions()
    with beam.Pipeline(options=options) as pipeline:
        data = (
            pipeline
            | 'Create Dummy Source' >> beam.Create([None])  # Trigger to start the pipeline
            | 'Read from Postgres' >> beam.ParDo(PostgresSource())
            | 'Write to MySQL' >> beam.ParDo(MySQLSink())
        )

if __name__ == '__main__':
    run_pipeline()
