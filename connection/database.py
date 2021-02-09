import psycopg2
from psycopg2.extras import RealDictCursor
from .configuration import ConnectionConfiguration
from .query import QueryResult


class DatabaseConnection:

    def __init__(self):
        self.is_connected = False

    def configure(self, conf: ConnectionConfiguration):
        self.__configuration = conf

    def execute(self, query: str) -> QueryResult:
        result = QueryResult(['None'])
        try:
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            self.cursor.execute(query)
            fetched = self.cursor.fetchall()
            result.change_query([dict(x) for x in fetched])
            print('SELECT was successful: ', query)
        except Exception as error:
            print("Error while querying to database: ", error, "\nWith query: ", query)
        return result

    def commit(self, query: str, query_type: str = 'QUERY'):
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(query)
            self.connection.commit()
            print(query_type, ' was successful: ', query)
        except Exception as error:
            print(error)
            self.cursor.execute("ROLLBACK")

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                user=self.__configuration.user,
                password=self.__configuration.password,
                host=self.__configuration.host,
                port=self.__configuration.port,
                database=self.__configuration.database
            )
            self.is_connected = True
        except Exception as error:
            print("Error while connecting to PostgreSQL", error)

    def close(self):
        if self.is_connected:
            self.cursor.close()
            self.connection.close()
            print("PostgreSQL connection is closed")
        else:
            print("There is no connection to database")