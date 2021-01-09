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

class TestFindByIdInt(unittest.TestCase):
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

class Person(Entity):
    _table_name = 'person_manager_test'
    second_name = field.Column(storetype.Text(max_length=80), name="second_name")
    first_name = field.Column(storetype.Text(max_length=30), name="first_name")

    def __init__(self):
        super().__init__()

class TestFindByIdStr(unittest.TestCase):
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

        entity = Person()
        cls.records = []

        for i in range(0,10):
            cls.records.append(deepcopy(entity))
            cls.records[i].first_name = "First " + str(i)
            cls.records[i].second_name = "SECOND " + str(i)
            cls.m.insert(cls.records[i])

    # Executed after all tests from this class
    @classmethod
    def tearDownClass(cls):
        for record in cls.records:
            cls.m.delete(record)

        cls.m.close()

    # Executed before every test from this class
    def setUp(self):
        self.record = Person()

    # Executed after every test from this class
    def tearDown(self):
        self.record = None

    def test_id_exists(self):
        self.record = self.m.find_by_id(self.record, "SECOND 2")
        correct = Person()
        correct.first_name = "First 2"
        correct.second_name = "SECOND 2"

        self.assertEqual(self.record.first_name, correct.first_name)
        self.assertEqual(self.record.second_name, correct.second_name)

    def test_id_not_exists(self):
        self.record = self.m.find_by_id(self.record, "143215")
        self.assertEqual(self.record, None)

    def test_wrong_id_type(self):
        self.assertRaises(AssertionError, self.m.find_by_id, self.record, 1)


class Vehicle(Entity):
    _table_name = 'vehicle_manager_test'
    model = field.Column(storetype.Text(max_length=80), name="second_name")
    make = field.Column(storetype.Text(max_length=30), name="first_name")
    engine_volume = field.PrimaryKey(storetype.Float(), name='engine_volume')

    def __init__(self):
        super().__init__()

class TestFindByIdCustomPk(unittest.TestCase):
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

        entity = Vehicle()
        cls.records = []

        for i in range(0,10):
            cls.records.append(deepcopy(entity))
            cls.records[i].model = "Model " + str(i)
            cls.records[i].make = "Tesla"
            cls.records[i].engine_volume = i + 2.5
            cls.m.insert(cls.records[i])

    # Executed after all tests from this class
    @classmethod
    def tearDownClass(cls):
        for record in cls.records:
            cls.m.delete(record)

        cls.m.close()

    # Executed before every test from this class
    def setUp(self):
        self.record = Vehicle()

    # Executed after every test from this class
    def tearDown(self):
        self.record = None

    def test_id_exists(self):
        self.record = self.m.find_by_id(self.record, 3.5)
        correct = Person()
        correct.model = "Model 1"
        correct.make = "Tesla"
        correct.engine_volume = 3.5

        self.assertEqual(self.record.model, correct.model)
        self.assertEqual(self.record.make, correct.make)
        self.assertEqual(self.record.engine_volume, correct.engine_volume)

    def test_id_not_exists(self):
        self.record = self.m.find_by_id(self.record, 1.43215)
        self.assertEqual(self.record, None)

    def test_wrong_id_type(self):
        self.assertRaises(AssertionError, self.m.find_by_id, self.record, 1)



if __name__ == '__main__':
    unittest.main()