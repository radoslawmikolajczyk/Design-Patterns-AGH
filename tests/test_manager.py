from entity.entity import Entity
import fields.field as field
import fields.storetype as storetype
import fields.relationship as rel
from connection.configuration import ConnectionConfiguration
from manager import Manager
from copy import deepcopy
import unittest

class City(Entity):
    _table_name = 'city_manager_test'
    id = field.PrimaryKey(storetype.Integer(), name='id')
    name = field.Column(storetype.Text(max_length=30), name='name')

class TestFindByIdInt(unittest.TestCase):
    # Executed before all tests in this class
    @classmethod
    def setUpClass(cls):
        entity = City()
        cls.records = []

        for i in range(0,10):
            cls.records.append(deepcopy(entity))
            cls.records[i].id = i
            cls.records[i].name = "CITY " + str(i)
            m.insert(cls.records[i])

    # Executed after all tests from this class
    @classmethod
    def tearDownClass(cls):
        for record in cls.records:
            m.delete(record)

    # Executed before every test from this class
    def setUp(self):
        self.record = City()

    # Executed after every test from this class
    def tearDown(self):
        self.record = None

    def test_id_exists(self):
        self.record = m.find_by_id(self.record, 2)
        correct = City()
        correct.id = 2
        correct.name = "CITY 2"

        self.assertEqual(self.record.id, correct.id)
        self.assertEqual(self.record.name, correct.name)

    def test_id_not_exists(self):
        self.record = m.find_by_id(self.record, 143215)
        self.assertEqual(self.record, None)

    def test_wrong_id_type(self):
        self.assertRaises(AssertionError, m.find_by_id, self.record, "a")

class Person(Entity):
    _table_name = 'person_manager_test'
    second_name = field.Column(storetype.Text(max_length=80), name="second_name")
    first_name = field.Column(storetype.Text(max_length=30), name="first_name")

class TestFindByIdStr(unittest.TestCase):
    # Executed before all tests in this class
    @classmethod
    def setUpClass(cls):
        entity = Person()
        cls.records = []

        for i in range(0,10):
            cls.records.append(deepcopy(entity))
            cls.records[i].first_name = "First " + str(i)
            cls.records[i].second_name = "SECOND " + str(i)
            m.insert(cls.records[i])

    # Executed after all tests from this class
    @classmethod
    def tearDownClass(cls):
        for record in cls.records:
            m.delete(record)

    # Executed before every test from this class
    def setUp(self):
        self.record = Person()

    # Executed after every test from this class
    def tearDown(self):
        self.record = None

    def test_id_exists(self):
        self.record = m.find_by_id(self.record, "SECOND 2")
        correct = Person()
        correct.first_name = "First 2"
        correct.second_name = "SECOND 2"

        self.assertEqual(self.record.first_name, correct.first_name)
        self.assertEqual(self.record.second_name, correct.second_name)

    def test_id_not_exists(self):
        self.record = m.find_by_id(self.record, "143215")
        self.assertEqual(self.record, None)

    def test_wrong_id_type(self):
        self.assertRaises(AssertionError, m.find_by_id, self.record, 1)


class Vehicle(Entity):
    _table_name = 'vehicle_manager_test'
    model = field.Column(storetype.Text(max_length=80), name="second_name")
    make = field.Column(storetype.Text(max_length=30), name="first_name")
    engine_volume = field.PrimaryKey(storetype.Float(), name='engine_volume')

class TestFindByIdCustomPk(unittest.TestCase):
    # Executed before all tests in this class
    @classmethod
    def setUpClass(cls):
        entity = Vehicle()
        cls.records = []

        for i in range(0,10):
            cls.records.append(deepcopy(entity))
            cls.records[i].model = "Model " + str(i)
            cls.records[i].make = "Tesla"
            cls.records[i].engine_volume = i + 2.5
            m.insert(cls.records[i])

    # Executed after all tests from this class
    @classmethod
    def tearDownClass(cls):
        for record in cls.records:
            m.delete(record)

    # Executed before every test from this class
    def setUp(self):
        self.record = Vehicle()

    # Executed after every test from this class
    def tearDown(self):
        self.record = None

    def test_id_exists(self):
        self.record = m.find_by_id(self.record, 3.5)
        correct = Person()
        correct.model = "Model 1"
        correct.make = "Tesla"
        correct.engine_volume = 3.5

        self.assertEqual(self.record.model, correct.model)
        self.assertEqual(self.record.make, correct.make)
        self.assertEqual(self.record.engine_volume, correct.engine_volume)

    def test_id_not_exists(self):
        self.record = m.find_by_id(self.record, 1.43215)
        self.assertEqual(self.record, None)

    def test_wrong_id_type(self):
        self.assertRaises(AssertionError, m.find_by_id, self.record, 1)


class Address(Entity):
    _table_name = 'address_manager_test'
    id = field.Column(storetype.Text(max_length=30), name='id', unique=True, nullable=False)
    person_fk = rel.ManyToOne('Person', "person_fk")


class TestFindByIdManyToOne(unittest.TestCase):
    # Executed before all tests in this class
    @classmethod
    def setUpClass(cls):
        entity = Person()
        cls.person_records = []

        for i in range(0,5):
            cls.person_records.append(deepcopy(entity))
            cls.person_records[i].first_name = "First " + str(i)
            cls.person_records[i].second_name = "SECOND " + str(i)
            m.insert(cls.person_records[i])
        
        entity = Address()
        cls.address_records = []

        for i in range(0,5):
            cls.address_records.append(deepcopy(entity))
            cls.address_records[i].id = str(i)
            if i % 2 == 0:
                cls.address_records[i].person_fk = "SECOND " + str(i)
            else:
                cls.address_records[i].person_fk = "SECOND " + str(i-1)
            m.insert(cls.address_records[i])

    # Executed after all tests from this class
    @classmethod
    def tearDownClass(cls):
        for record in cls.person_records:
            m.delete(record)
        for record in cls.address_records:
            m.delete(record)

    # Executed before every test from this class
    def setUp(self):
        self.record = Address()

    # Executed after every test from this class
    def tearDown(self):
        self.record = None

    def testForeignKey(self):
        self.record = m.find_by_id(self.record, "3")
        self.assertEqual(self.record.person_fk, "SECOND 2")
        self.record = m.find_by_id(self.record, "2")
        self.assertEqual(self.record.person_fk, "SECOND 2")


class Actor(Entity):
    _table_name = 'actor_manager_test'
    id = field.PrimaryKey(storetype.Integer(), name='id')
    name = field.Column(storetype.Text(max_length=30), name='name')
    film_fk = rel.ManyToMany('Film', name='film_fk')


class Film(Entity):
    _table_name = 'film_manager_test'
    id = field.PrimaryKey(storetype.Integer(), name='id')
    title = field.Column(storetype.Text(max_length=30), name='title')
    actor_fk = rel.ManyToMany('Actor', name='actor_fk')


class TestFindByIdManyToMany(unittest.TestCase):
    # Executed before all tests in this class
    @classmethod
    def setUpClass(cls):
        entity = Film()
        film_records = []

        for i in range(0,5):
            film_records.append(deepcopy(entity))
            film_records[i].id = i
            film_records[i].title = 'Film ' + str(i)
            if i == 0:
                film_records[i].actor_fk = [i]
            else:
                film_records[i].actor_fk = [i, i-1]

        entity = Actor()
        actor_records = []

        for i in range(0,5):
            actor_records.append(deepcopy(entity))
            actor_records[i].id = i
            actor_records[i].name = 'Actor ' + str(i)
            if i == 0:
                actor_records[i].film_fk = [i]
            else:
                actor_records[i].film_fk = [i, i-1]
        
        cls.records = []
        for i in film_records:
            cls.records.append(deepcopy(i))
        for i in actor_records:
            cls.records.append(deepcopy(i))
        
        m.multi_insert(cls.records)

    # Executed after all tests from this class
    @classmethod
    def tearDownClass(cls):
        for record in cls.records:
            m.delete(record)

    # Executed before every test from this class
    def setUp(self):
        self.record = Actor()

    # Executed after every test from this class
    def tearDown(self):
        self.record = None

    def testForeignKey(self):
        self.record = m.find_by_id(self.record, 3)
        self.assertEqual(sorted(self.record.film_fk), sorted([3, 2, 4]))


class Vehicle(Entity):
    _table_name = 'vehicle_manager_test'
    model = field.Column(storetype.Text(max_length=80), name="second_name")
    make = field.Column(storetype.Text(max_length=30), name="first_name")
    engine_volume = field.PrimaryKey(storetype.Float(), name='engine_volume')

class TestFindBy(unittest.TestCase):
    # Executed before all tests in this class
    @classmethod
    def setUpClass(cls):
        entity = Vehicle()
        cls.records = []

        for i in range(0,10):
            cls.records.append(deepcopy(entity))
            cls.records[i].model = "Model " + str(i)
            cls.records[i].make = "Tesla"
            cls.records[i].engine_volume = i + 2.5
            m.insert(cls.records[i])

    # Executed after all tests from this class
    @classmethod
    def tearDownClass(cls):
        for record in cls.records:
            m.delete(record)

    # Executed before every test from this class
    def setUp(self):
        self.record = Vehicle()

    # Executed after every test from this class
    def tearDown(self):
        self.record = None

    def testUniqueField(self):
        self.record = m.find_by(self.record, 'model', 'Model 1')
        self.assertEqual(len(self.record), 1)
        correct = Vehicle()
        correct.model = "Model 1"
        correct.make = "Tesla"
        correct.engine_volume = 3.5
        self.assertEqual(self.record[0].model, correct.model)
        self.assertEqual(self.record[0].make, correct.make)
        self.assertEqual(self.record[0].engine_volume, correct.engine_volume)

    def testValueNotExists(self):
        self.record = m.find_by(self.record, 'model', 'Model 131414511')
        self.assertEqual(len(self.record), 0)
        self.assertEqual(self.record, [])
    
    def testMultipleMatches(self):
        self.record = m.find_by(self.record, 'make', 'Tesla')
        self.assertEqual(len(self.record), 10)


if __name__ == '__main__':
    m = Manager()
    conf = ConnectionConfiguration(user="postgres",
                                password="rajka1001",
                                database="postgres")
    m.connect(conf)
    m.create_tables()
    unittest.main()
    m.close()
