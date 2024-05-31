import apache_beam as beam
import psycopg2
import mysql.connector
from apache_beam.options.pipeline_options import PipelineOptions
from psycopg2 import OperationalError
from mysql.connector import Error

# Define the database connection parameters for PostgreSQL and MySQL
postgres_config = {
    'host': 'localhost',
    'dbname': 'demo',
    'user': 'postgres',
    'password': 'Momnoor9696@!',
    'port': 5432
}

mysql_config = {
    'host': 'localhost',
    'database': 'Parks_and_Recreation',
    'user': 'root',
    'password': '',
    'port': 3306
}

class PostgresSource(beam.DoFn):
    def process(self, element):
        try:
            postgres_conn = psycopg2.connect(**postgres_config)
            cursor = postgres_conn.cursor()
            cursor.execute('SELECT * FROM public.demo_table')
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

            # Check if the record already exists
            cursor.execute(
                'SELECT COUNT(*) FROM transferred_table WHERE id = %s',
                (element[0],)
            )
            count = cursor.fetchone()[0]

            if count > 0:
                # Update the record if it exists
                cursor.execute(
                    'UPDATE transferred_table SET name = %s, address = %s '
                    'WHERE id = %s',
                    (element[1], element[2], element[0])
                )
                print(f'Updated dataset with ID {element[0]}')
            else:
                # Insert the record if it does not exist
                cursor.execute(
                    'INSERT INTO transferred_table (id, name, address) '
                    'VALUES (%s, %s, %s)',
                    (element[0], element[1], element[2])
                )
                print(f'Inserted dataset with ID {element[0]}')

            mysql_conn.commit()
        except Error as e:
            print(f'Error connecting to MySQL: {e}')
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'mysql_conn' in locals():
                mysql_conn.close()

def run_pipeline():
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

if __name__ == '__main__':
    run_pipeline()
