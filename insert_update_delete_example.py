from entity.entity import Entity
import fields.field as field
import fields.storetype as storetype
import fields.relationship as rel
from connection.configuration import ConnectionConfiguration
from manager import Manager

############################################################################################

# WARNING: these classes are different from the ones in create_tables_example.py
# so in order to run this file without any errors you should drop schema first

############################################################################################

class Person(Entity):
    # _table_name = 'osoba'
    _first_name = field.Column(storetype.Text(max_length=30), name="first_name")
    _second_name = field.Column(storetype.Text(max_length=80), name="second_name")

class Address(Entity):
    # _table_name = 'adres'
    id = field.Column(storetype.Text(max_length=30), name='id', unique=True, nullable=False)
    person_fk = rel.ManyToOne('Person', "person_fk")

class City(Address):
    _table_name = 'miasto'
    id = field.PrimaryKey(storetype.Integer(), name='id')
    name = field.Column(storetype.Text(max_length=30), name='name')
    address_fk = rel.OneToOne('Address', name='address_fk')


m = Manager()
conf = ConnectionConfiguration(user="postgres",
                               password="rajka1001",
                               database="postgres")
m.connect(conf)
m.create_tables()

p = Person()
p._first_name = "person1"
p._second_name = "sur1"

a1 = Address()
a1.id = "a"
a1.person_fk = "person1"

a2 = Address()
a2.id = "b"
a2.person_fk = "person1"


# TEST 1 ##########################################################

m.insert(p)
m.insert(a1)
m.insert(a2)

# TODO: implement functionality allowing to update the primary key of a table
#       when it is used as foreign key in other tables
# p._first_name = "changeId"
# m.update(p)

m.delete(a1)
m.delete(a2)
m.delete(p)


# TEST 2 ##########################################################

c = City()
c.id = 1
c.name = "New York"
c.address_fk = 'a'

# executing the following code (inserting c2 object) should cause ERROR -> and so it is :)
# c2 = City()
# c2.id = 2
# c2.name = "London"
# c2.address_fk = 'a'

m.insert(p)
m.insert(a1)
m.insert(c)
# m.insert(c2)

m.delete(c)
# m.delete(c2)
m.delete(a1)
m.delete(p)


m.close()