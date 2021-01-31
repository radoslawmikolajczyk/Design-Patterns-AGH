from entity.entity import Entity
import fields.field as field
import fields.storetype as storetype
import fields.relationship as rel
from connection.configuration import ConnectionConfiguration
from manager import Manager


class Country(Entity):
    _table_name = 'panstwo'
    country_name = field.Column(storetype.Text(max_length=30))


class City(Country):
    _table_name = 'miasto'
    city_name = field.Column(storetype.Text(max_length=30), name="city_name")


class Address(City):
    _table_name = 'address'
    address_id = field.Column(storetype.Text(max_length=30), unique=True, nullable=False, name='addr_id')
    street_name = field.Column(storetype.Text(max_length=80))
    home_nr = field.Column(storetype.Integer(), name='home_nr')


m = Manager()
conf = ConnectionConfiguration(user="postgres",
                               password="rajka1001",
                               database="postgres")
m.connect(conf)
a = Address()
a.address_id = "Ulica Czarnowiejska"
a.country_name = "Polska"
a.city_name = "Kraków"

m.get_inheritance_data(a)