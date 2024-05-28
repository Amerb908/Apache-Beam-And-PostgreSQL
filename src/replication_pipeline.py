import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import psycopg2


#This replicates the pipeline from one database to another in PostgresSQL

class ReadFromPostgres(beam.DoFn):
    def __init__(self, db_config):
        self.db_config = db_config

    def start_bundle(self):
        self.conn = psycopg2.connect(**self.db_config)
        self.cursor = self.conn.cursor()

    def process(self, element):
        self.cursor.execute("SELECT * FROM public.demo_table")
        rows = self.cursor.fetchall()
        for row in rows:
            yield row

    def finish_bundle(self):
        self.cursor.close()
        self.conn.close()

class WriteToPostgres(beam.DoFn):
    def __init__(self, db_config):
        self.db_config = db_config

    def start_bundle(self):
        self.conn = psycopg2.connect(**self.db_config)
        self.cursor = self.conn.cursor()

    def process(self, element):
        insert_query = "INSERT INTO public.replicas_table (id, name, address) VALUES (%s, %s, %s)"
        self.cursor.execute(insert_query, element)  # Assuming element is a tuple with three values
        self.conn.commit()

    def finish_bundle(self):
        self.cursor.close()
        self.conn.close()
def run():
    source_db_config = {
        'host': 'localhost',
        'dbname': 'demo',
        'user': 'postgres',
        'password': '',
        'port': 5432
    }
    
    target_db_config = {
        'host': 'localhost',
        'dbname': 'Database2',
        'user': 'postgres',
        'password': '',
        'port': 5432
    }

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    p1 = beam.Pipeline(options=options)

    (p1
     | 'ReadFromPostgres' >> beam.Create([None])
     | 'FetchData' >> beam.ParDo(ReadFromPostgres(source_db_config))
     | 'WriteToPostgres' >> beam.ParDo(WriteToPostgres(target_db_config))
    )

    result = p1.run()
    result.wait_until_finish()

if __name__ == '__main__':
    run()
