from entity.entity import Entity
import fields.field as field
import fields.storetype as storetype
from connection.configuration import ConnectionConfiguration
from manager import Manager
from copy import deepcopy
import unittest

class City(Entity):
    _table_name = 'city_manager_test'
    id = field.PrimaryKey(storetype.Integer(), name='id')
    name = field.Column(storetype.Text(max_length=30), name='name')

    def __init__(self):
        super().__init__()

class TestFindById(unittest.TestCase):
    # Executed before all tests in this class
    @classmethod
    def setUpClass(cls):
        cls.m = Manager()
        conf = ConnectionConfiguration(user="postgres",
                                    password="rajka1001",
                                    database="postgres")
        cls.m.connect(conf)
        cls.m.create_tables()
        cls.records = []

        entity = City()
        cls.records = []

        for i in range(0,10):
            cls.records.append(deepcopy(entity))
            cls.records[i].id = i
            cls.records[i].name = "CITY " + str(i)
            cls.m.insert(cls.records[i])

    # Executed after all tests from this class
    @classmethod
    def tearDownClass(cls):
        for record in cls.records:
            cls.m.delete(record)

        cls.m.close()

    # Executed before every test from this class
    def setUp(self):
        self.record = City()

    # Executed after every test from this class
    def tearDown(self):
        self.record = None

    def test_id_exists(self):
        self.record = self.m.find_by_id(self.record, 2)
        correct = City()
        correct.id = 2
        correct.name = "CITY 2"

        self.assertEqual(self.record.id, correct.id)
        self.assertEqual(self.record.name, correct.name)

    def test_id_not_exists(self):
        self.record = self.m.find_by_id(self.record, 143215)
        self.assertEqual(self.record, None)

    def test_wrong_id_type(self):
        self.assertRaises(AssertionError, self.m.find_by_id, self.record, "a")

if __name__ == '__main__':
    unittest.main()