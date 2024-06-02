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

# DoFn that reads data from PostgreSQL
class PostgresSource(beam.DoFn):
    def process(self, element):
        try:
            # Establish a connection to the PostgreSQL database
            postgres_conn = psycopg2.connect(**postgres_config)
            cursor = postgres_conn.cursor()

            # Execute a query to fetch all rows from the demo_table
            cursor.execute('SELECT * FROM public.demo_table')
            
            # Yield each row fetched from the database
            for row in cursor.fetchall():
                yield row
        except OperationalError as e:
            # If there is an error connecting to the database, print the error message
            print(f'Error connecting to PostgreSQL: {e}')
        finally:
            # Close the cursor and connection to the database
            if 'cursor' in locals():
                cursor.close()
            if 'postgres_conn' in locals():
                postgres_conn.close()

class MySQLSink(beam.DoFn):
    def process(self, element):
        try:
            # Establish a connection to the MySQL database
            mysql_conn = mysql.connector.connect(**mysql_config)
            
            # Create a cursor object to interact with the database
            cursor = mysql_conn.cursor()

            # Check if the record with the specified ID already exists in the demo_table
            cursor.execute(
                'SELECT COUNT(*) FROM demo_table WHERE id = %s',  # SQL query to count records with the specified ID
                (element[0],)  # tuple containing the ID to check for
            )

            # Fetch the result of the query (i.e., the count of records with the specified ID)
            count = cursor.fetchone()[0]

            if count > 0:
                # If the record exists, update its name and address fields
                cursor.execute(
                    'UPDATE demo_table SET name = %s, address = %s '
                    'WHERE id = %s',  # SQL query to update the record
                    (element[1], element[2], element[0])  # tuple containing the new name, address, and ID
                )
                print(f'Updated dataset with ID {element[0]}')  # Print a message indicating that the record was updated
            else:
                # If the record does not exist, insert a new record with the specified ID, name, and address
                cursor.execute(
                    'INSERT INTO demo_table (id, name, address) '
                    'VALUES (%s, %s, %s)',  # SQL query to insert a new record
                    (element[0], element[1], element[2])  # tuple containing the ID, name, and address
                )
                print(f'Inserted dataset with ID {element[0]}')  # Print a message indicating that the record was inserted

            # Commit the changes made to the database
            mysql_conn.commit()
        except Error as e:
            print(f'Error connecting to MySQL: {e}')  # Print an error message if there is an error connecting to the MySQL database
        finally:
            if 'cursor' in locals():
                cursor.close()  # Close the cursor object
            if 'mysql_conn' in locals():
                mysql_conn.close()  # Close the connection to the MySQL database

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
