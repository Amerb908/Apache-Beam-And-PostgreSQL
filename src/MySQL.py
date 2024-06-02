# Using Apache Beam to transfer data a CSV file to MySQL
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd
import mysql.connector
from mysql.connector import Error

class MySQLConnector:
    def __init__(self, hostname, database, username, password, port):
        self.hostname = hostname
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.hostname,
                database=self.database,
                user=self.username,
                password=self.password,
                port=self.port
            )
            print('Connected to the database')
            return self.connection
        except Error as e:
            print(f"Error: {e}")
            return None

    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Disconnected from the database.")

class WriteToMySQL(beam.DoFn):
    def __init__(self, hostname, database, username, password, port, table_name):
        self.hostname = hostname
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.table_name = table_name

    def start_bundle(self):
        self.db_connector = MySQLConnector(
            self.hostname, self.database, self.username, self.password, self.port)
        self.connection = self.db_connector.connect()
        self.cursor = self.connection.cursor()

    def process(self, element):
        try:
            columns = ', '.join([f'`{col}`' for col in element.keys()])
            placeholders = ', '.join(['%s'] * len(element))
            update_stmt = ', '.join([f'`{col}`=VALUES(`{col}`)' for col in element.keys() if col != 'Index'])
            insert_stmt = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_stmt}"
            self.cursor.execute(insert_stmt, tuple(element.values()))
            self.connection.commit()
        except Error as e:
            print(f"Error: {e}")

    def finish_bundle(self):
        self.db_connector.disconnect()

def run_pipeline(csv_file_path, table_name):
    options = PipelineOptions()
    p = beam.Pipeline(options=options)

    df = pd.read_csv(csv_file_path)
    data = df.to_dict(orient='records')

    data_pcoll = p | 'Create PCollection' >> beam.Create(data)

    _ = data_pcoll | 'Write to MySQL' >> beam.ParDo(WriteToMySQL(
        hostname='localhost',
        database='Demo_Schema',
        username='root',
        password='',
        port=3306,
        table_name=table_name
    ))

    p.run().wait_until_finish()
    
def main():
    csv_file_path = 'Data/customers-100.csv'
    table_name = 'customer_data'
    run_pipeline(csv_file_path, table_name)
    

if __name__ == "__main__":
    main()
