from connection.database import DatabaseConnection
from connection.configuration import ConnectionConfiguration
from connection.query import QueryResult

import unittest


class ConnectionTests(unittest.TestCase):

    def test_query(self):
        query = QueryResult("SELECT * FROM test")
        query.change_query("SELECT id FROM test")
        self.assertEqual("SELECT id FROM test", query.get_query())

    def test_configuration(self):
        conf = ConnectionConfiguration(user='test', password='test', database="postgres")
        self.assertEqual(conf.host, '127.0.0.1')
        self.assertEqual(conf.port, '5432')

