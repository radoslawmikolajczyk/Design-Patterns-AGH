from entity.entity import Entity
import fields.field as field
import fields.storetype as storetype
from connection.configuration import ConnectionConfiguration
from manager import Manager


class Person(Entity):
    first_name = field.Column(storetype.Text(max_length=30), name="first_name")
    second_name = field.Column(storetype.Text(max_length=80), name="second_name")

    def __init__(self, name):
        super().__init__(name)


class Address(Entity):
    xd = field.Column(storetype.Text(max_length=30), name="xd")

    def __init__(self, name):
        super().__init__(name)


class City(Address):
    id = field.PrimaryKey(storetype.Integer(), name='id')

    def __init__(self, name):
        super().__init__(name)


p = Person('people')
p1 = Address('address')
p2 = City('city')
m = Manager()
conf = ConnectionConfiguration(user="postgres",
                               password="rajka1001",
                               database="postgres")
m.connect(conf)
m.create_tables()
