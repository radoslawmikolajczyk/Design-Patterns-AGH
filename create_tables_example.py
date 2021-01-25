from entity.entity import Entity
import fields.field as field
import fields.storetype as storetype
import fields.relationship as rel
from connection.configuration import ConnectionConfiguration
from manager import Manager


class Person(Entity):
    # _table_name = 'osoba'
    _first_name = field.Column(storetype.Text(max_length=30), name="first_name")
    _second_name = field.Column(storetype.Text(max_length=80), name="second_name")


class Address(Entity):
    # _table_name = 'address'
    xd = field.Column(storetype.Text(max_length=30), name='xd2', unique=True, nullable=False)
    cos = rel.ManyToOne('Person', "pppeeerson")


class City(Address):
    _table_name = 'city1'
    city_id = field.PrimaryKey(storetype.Integer())
    name = field.Column(storetype.Text(max_length=30), name='name')
    address = rel.OneToOne('Address', name='address_id')


class Test(Entity):
    _table_name = 'test1'
    test_id = field.PrimaryKey(storetype.Integer())
    name = field.Column(storetype.Text(max_length=30))
    test1_id = rel.ManyToMany('Test2')


class Test2(Entity):
    _table_name = 'test2'
    address = rel.ManyToMany('Test', name='test2_id')
    id = field.PrimaryKey(storetype.Integer(), name='id')
    name = field.Column(storetype.Text(max_length=30), name='name')



m = Manager()
conf = ConnectionConfiguration(user="postgres",
                               password="rajka1001",
                               database="postgres")
m.connect(conf)
m.create_tables()



# simple example for insert, update, delete
p = Person()
p._first_name = "12345"
p._second_name = "second"
# m.insert(p)
# p._second_name = "secondsecond"
# m.update(p)
# m.delete(p)
#
# p2.id = 1
# p2.name = "namep2"
# m.insert(p2)
# # p2.name ="upadtename"
# # m.update(p2)
# m.delete(p2)

ad = Address()
ad.xd = "aaaa"
ad.cos = "12345"
# m.insert(ad)
# m.delete(ad)
m.close()
