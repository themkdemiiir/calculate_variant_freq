import psycopg2
from psycopg2 import Error


class Database:
    def __init__(self, username, password, database):
        self.username = username
        self.password = password
        self.database = database
        try:
            self.connection = psycopg2.connect(user=f"{self.username}",
                                               password=f"{self.password}",
                                               host="localhost",
                                               database=f"{self.database}")
            self.cursor = self.connection.cursor()

        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)

    def currently_existing_tables(self):
        self.cursor.execute("""SELECT table_name FROM information_schema.tables
                       WHERE table_schema = 'public'""")

        table_names = []
        for table in self.cursor.fetchall():
            table_names.append(table[0])
        return table_names

    def query(self, query):
        try:
            self.cursor.execute(query)
            self.connection.commit()
            print("Query is executed")
        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)

    def close_connection(self):
        self.cursor.close()
        self.connection.close()
        print("PostgreSQL connection is closed")

    class TableAlreadyExists(Exception):
        def __init__(self, message='The table you want to create is already exists'):
            self.message = message

        def __str__(self):
            return self.message

    class TableCannotBeFound(Exception):
        def __init__(self, message='The table you want to update cannot be found'):
            self.message = message

        def __str__(self):
            return self.message