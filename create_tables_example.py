from entity.entity import Entity
import fields.field as field
import fields.storetype as storetype
from connection.configuration import ConnectionConfiguration
from manager import Manager


class Person(Entity):
    _table_name = 'osoba'
    _first_name = field.Column(storetype.Text(max_length=30), name="first_name")
    _second_name = field.Column(storetype.Text(max_length=80), name="second_name")

    def __init__(self):
        super().__init__()


class Address(Entity):
    _table_name = 'address'
    xd = field.Column(storetype.Text(max_length=30), name="xd", unique=True, nullable=False)

    def __init__(self):
        super().__init__()


class City(Address):
    _table_name = 'city1'
    id = field.PrimaryKey(storetype.Integer(), name='id')
    name = field.Column(storetype.Text(max_length=30), name='name')

    def __init__(self):
        super().__init__()

m = Manager()
conf = ConnectionConfiguration(user="postgres",
                               password="rajka1001",
                               database="postgres")
m.connect(conf)
m.create_tables()

p = Person()
p1 = Address()
p2 = City()

# simple example for insert, update, delete
p._first_name = "12345"
p._second_name = "second"
m.insert(p)
#p._second_name = "secondsecond"
#m.update(p)
m.delete(p)

# p2.id = 1
# p2.name = "namep2"
# m.insert(p2)
# m.delete(p2)

m.close()
