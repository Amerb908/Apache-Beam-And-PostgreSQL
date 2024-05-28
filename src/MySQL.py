#this file is to test if the mysql connector works. It does. 
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
        self.cursor = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.hostname,
                database=self.database,
                user=self.username,
                password=self.password,
                port=self.port
            )
            if self.connection.is_connected():
                self.cursor = self.connection.cursor()
                print("Connected to the database!")
        except Error as e:
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
            except Error as e:
                print(f"Error: {e}")
        else:
            print("No active database connection.")
        return None

def main():
    # Define the database connection parameters
    hostname = 'localhost'
    database = 'Parks_and_Recreation'
    username = 'root'
    password = ''
    port = 3306  # default MySQL port

    # Initialize the connector
    db_connector = MySQLConnector(hostname, database, username, password, port)

    # Connect to the database
    db_connector.connect()

    # Define a query to test the connection
    test_query = "SELECT * FROM employee_demographics;"

    # Execute the query and fetch results
    results = db_connector.execute_query(test_query, fetch_results=True)
    if results:
        for row in results:
            print(row)

    # Disconnect from the database
    db_connector.disconnect()

if __name__ == "__main__":
    main()
