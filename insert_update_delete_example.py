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


class Person(Entity):  # simple class
    # _table_name = 'osoba'
    _first_name = field.Column(storetype.Text(max_length=30), name="first_name")
    _second_name = field.Column(storetype.Text(max_length=80))


class Address(Entity):  # many to one relation
    # _table_name = 'adres'
    id = field.Column(storetype.Text(max_length=30), name='id', unique=True, nullable=False)
    person_fk = rel.ManyToOne('Person', "person_fk")


class City(Entity):  # one to one relation
    _table_name = 'miasto'
    id = field.PrimaryKey(storetype.Integer(), name='id')
    name = field.Column(storetype.Text(max_length=30), name='name')
    address_fk = rel.OneToOne('Address', name='address_fk')


class Actor(Entity):  # many to many relation
    id = field.PrimaryKey(storetype.Integer(), name='id')
    name = field.Column(storetype.Text(max_length=30), name='name')
    film_fk = rel.ManyToMany('Film', name='film_fk')


class Film(Entity):  # many to many relation
    id = field.PrimaryKey(storetype.Integer(), name='id')
    title = field.Column(storetype.Text(max_length=30), name='title')
    actor_fk = rel.ManyToMany('Actor', name='actor_fk')


class Book(Entity):  # multiple many to many relation
    id = field.PrimaryKey(storetype.Integer(), name='id')
    title = field.Column(storetype.Text(max_length=30), name='title')
    author_fk = rel.ManyToMany('Author', name='author_fk')


class Author(Entity):  # multiple many to many relation
    id = field.PrimaryKey(storetype.Integer(), name='id')
    name = field.Column(storetype.Text(max_length=30), name='name')
    book_fk = rel.ManyToMany('Book', name='book_fk')
    poem_fk = rel.ManyToMany('Poem')


class Poem(Entity):  # multiple many to many relation
    id = field.PrimaryKey(storetype.Integer(), name='id')
    title = field.Column(storetype.Text(max_length=30), name='title')
    author_fk = rel.ManyToMany('Author', name='author_fk')


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
a1.person_fk = p

a2 = Address()
a2.id = "b"
a2.person_fk = p

c = City()
c.id = 1
c.name = "New York"
c.address_fk = a1

actor1 = Actor()
actor2 = Actor()

film1 = Film()
film2 = Film()

actor1.id = 1
actor1.name = "name1"
actor1.film_fk = [film1, film2]

actor2.id = 2
actor2.name = "name2"
actor2.film_fk = [film1, film2]

film1.id = 1
film1.title = "title1"
film1.actor_fk = [actor1, actor2]

film2.id = 2
film2.title = "title2"
film2.actor_fk = [actor1, actor2]

# EXAMPLE 1 ######################## simple many to one relation with update ##################################

m.insert(p)
m.insert(a1)
m.insert(a2)

p._first_name = "changedFK"
m.update(p)

# to_update = Person()
# to_update = m.find_by_id(to_update, "changedFK")
# m.insert(to_update)
# to_update._second_name = "From findby"
# m.update(to_update)
#
# selected = m.select(Address(), "SELECT * FROM address")
# print("\nSelect found: ", selected)
#
# for i in selected:
#     print("Address id:", i.id, "; It's an address of person with first name:", i.person_fk)
# print("\n")

m.delete(a1)
m.delete(a2)
m.delete(p)

# EXAMPLE 2 ######################### simple one to one relation ##############################################

# executing the following code (inserting c2 object) should cause ERROR -> and so it is :)
# c2 = City()
# c2.id = 2
# c2.name = "London"
# c2.address_fk = 'a'

a1.person_fk = p

m.insert(p)
m.insert(a1)
m.insert(c)
# m.insert(c2)

m.delete(c)
# m.delete(c2)
m.delete(a1)
m.delete(p)

# EXAMPLE 3 ########################## simple multi insert #####################################################

# multi insert is used for inserting objects with many to many relation
m.multi_insert([p, a1, c])
m.delete(c)
m.delete(a1)
m.delete(p)

# EXAMPLE 4 ########################## simple multi insert for ManyToMany relation ###########################

m.multi_insert([actor1, actor2, film1, film2])

film2.id = 222
m.update(film2)

m.delete(actor1)
m.delete(actor2)
m.delete(film1)
m.delete(film2)

# EXAMPLE 5 ########################## insert and update (with some NULL fields) ###############################

city = City()
city.id = 1
city.name = None
# city.address_fk <-- this field is not assigned

m.insert(city)
m.update(city)
m.delete(city)

# EXAMPLE 6 ########################## multiple ManyToMany relations ##########################################

b1 = Book()
b2 = Book()
a1 = Author()
a2 = Author()
p1 = Poem()
p2 = Poem()

b1.id = 1
b1.title = "btitle1"
b1.author_fk = [a1]

b2.id = 2
b2.title = "btitle2"
b2.author_fk = [a1]

p1.id = 1
p1.title = "ptitle1"
p1.author_fk = [a1, a2]

p2.id = 2
p2.title = "ptitle2"
p2.author_fk = [a1, a2]

a1.id = 1
a1.name = "name1"
a1.book_fk = [b1, b2]
a1.poem_fk = [p1, p2]

a2.id = 2
a2.name = "name2"
a2.poem_fk = [p1, p2]

m.multi_insert([b1, b2, a1, a2, p1, p2])

# find = Author()
# find = m.find_by(find, 'name', 'name1')
# print("\nFound authors: ", find)
# found = find[0]
# print("I found this author: Name:", found.name, "; Books:", found.book_fk, "; Poems:", found.poem_fk, "\n")
# found.name = "Changed"
# m.update(found)

m.delete(a1)
m.delete(a2)
m.delete(b1)
m.delete(b2)
m.delete(p1)
m.delete(p2)

m.close()