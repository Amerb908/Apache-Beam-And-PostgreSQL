import psycopg2
from psycopg2 import OperationalError


class PostgresConnector:
    def __init__(self, hostname, database, username, password, port):
        self.hostname = hostname
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.hostname,
                dbname=self.database,
                user=self.username,
                password=self.password,
                port=self.port
            )
            self.cursor = self.connection.cursor()
            print("Connected to the database!")
        except OperationalError as e:
            print(f"Error: {e}")

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Disconnected from the database.")

    def execute_query(self, query, fetch_results=False):
        if self.connection and self.cursor:
            try:
                self.cursor.execute(query)
                if fetch_results:
                    results = self.cursor.fetchall()
                    return results
                else:
                    self.connection.commit()
                    print("Query executed successfully.")
            except OperationalError as e:
                print(f"Error: {e}")
        else:
            print("No active database connection.")
        return None


def main():
    # Define the database connection parameters
    hostname = 'localhost'
    database = 'demo'
    username = 'postgres'
    password = '*************'
    port = 5432

    # Initialize the connector
    db_connector = PostgresConnector(hostname, database, username, password, port)

    # Connect to the database
    db_connector.connect()

    # Define a query
    select_query = "SELECT * FROM public.demo_table"

    # Execute the query and fetch results
    results = db_connector.execute_query(select_query, fetch_results=True)
    if results:
        for row in results:
            print(row)

    # Disconnect from the database
    db_connector.disconnect()


if __name__ == "__main__":
    main()
