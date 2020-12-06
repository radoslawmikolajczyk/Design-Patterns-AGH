import psycopg2
from .configuration import ConnectionConfiguration
from .query import QueryResult


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class DatabaseConnection(metaclass=SingletonMeta):

    def configure(self, conf: ConnectionConfiguration):
        self.__configuration = conf

    def execute(self, query: str) -> QueryResult:
        result = QueryResult(['None'])
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(query)
            result.change_query(self.cursor.fetchall())
        except Exception as error:
            print("Error while executing to database", error)
        return result

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                user=self.__configuration.user,
                password=self.__configuration.password,
                host=self.__configuration.host,
                port=self.__configuration.port,
                database=self.__configuration.database
            )
        except Exception as error:
            print("Error while connecting to PostgreSQL", error)

    def close(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print("PostgreSQL connection is closed")