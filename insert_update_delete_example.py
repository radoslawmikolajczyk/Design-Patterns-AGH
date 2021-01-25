from entity.entity import Entity
import fields.field as field
import fields.storetype as storetype
import fields.relationship as rel
from connection.configuration import ConnectionConfiguration
from manager import Manager

############################################################################################

# WARNING: these classes are different from the ones in create_tables_example.py
# so in order to run this file without any errors you should drop schema first

'''
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
'''

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


class Actor(Entity):
    id = field.PrimaryKey(storetype.Integer(), name='id')
    name = field.Column(storetype.Text(max_length=30), name='name')
    film_fk = rel.ManyToMany('Film', name='film_fk')


class Film(Entity):
    id = field.PrimaryKey(storetype.Integer(), name='id')
    title = field.Column(storetype.Text(max_length=30), name='title')
    actor_fk = rel.ManyToMany('Actor', name='actor_fk')


# CONFIGURATION

m = Manager()
conf = ConnectionConfiguration(user="postgres",
                               password="rajka1001",
                               database="postgres")
m.connect(conf)
m.create_tables()

# OBJECTS

p = Person()
p._first_name = "person1"
p._second_name = "sur1"

a1 = Address()
a1.id = "a"
a1.person_fk = "person1"

a2 = Address()
a2.id = "b"
a2.person_fk = "person1"

c = City()
c.id = 1
c.name = "New York"
c.address_fk = 'a'

actor1 = Actor()
actor2 = Actor()

film1 = Film()
film2 = Film()

actor1.id = 1
actor1.name = "name1"
actor1.film_fk = [1, 2]

actor2.id = 2
actor2.name = "name2"
actor2.film_fk = [1, 2]

film1.id = 1
film1.title = "title1"
film1.actor_fk = [1, 2]

film2.id = 2
film2.title = "title2"
film2.actor_fk = [1, 2]


# EXAMPLE 1 ######################## simple many to one relation with update ##################################

m.insert(p)
m.insert(a1)
m.insert(a2)

p._first_name = "changedFK"
m.update(p)

m.delete(a1)
m.delete(a2)
m.delete(p)

# EXAMPLE 2 ######################### simple one to one relation ##############################################

# executing the following code (inserting c2 object) should cause ERROR -> and so it is :)
# c2 = City()
# c2.id = 2
# c2.name = "London"
# c2.address_fk = 'a'

a1.person_fk = "changedFK"

m.insert(p)
m.insert(a1)
m.insert(c)
# m.insert(c2)

m.delete(c)
# m.delete(c2)
m.delete(a1)
m.delete(p)

# EXAMPLE 3 ########################## simple multi insert #####################################################

m.multi_insert([p, a1, c])
m.delete(c)
m.delete(a1)
m.delete(p)

# EXAMPLE 4 ########################## simple multi insert for many to many relation ###########################

m.multi_insert([actor1, actor2, film1, film2])

film2.id = 222
m.update(film2)

m.delete(actor1)
m.delete(actor2)
m.delete(film1)
m.delete(film2)

m.close()