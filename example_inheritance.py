from entity.entity import Entity
import fields.field as field
import fields.storetype as storetype
import fields.relationship as rel
from connection.configuration import ConnectionConfiguration
from manager import Manager


class City(Entity):
    _table_name = 'miasto'
    city_name = field.Column(storetype.Text(max_length=30))


class Address(City):
    address_id = field.Column(storetype.Text(max_length=30), unique=True, nullable=False)
    street_name = field.Column(storetype.Text(max_length=80))
    home_nr = field.Column(storetype.Integer(), name='home_nr')


m = Manager()
conf = ConnectionConfiguration(user="postgres",
                               password="rajka1001",
                               database="postgres")
m.connect(conf)
a = Address()
a2 = Address()

print(a.city_name)
print(a2.city_name)


