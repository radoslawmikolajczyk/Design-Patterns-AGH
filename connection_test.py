from connection.database import DatabaseConnection
from connection.configuration import ConnectionConfiguration


if __name__ == '__main__':
    conf = ConnectionConfiguration(user="postgres",
                                   password="rajka1001",
                                   database="postgres")
    connection = DatabaseConnection()
    connection.configure(conf)
    connection.connect()
    result = connection.execute("SELECT version();")
    print(result.get_query())
    connection.close()