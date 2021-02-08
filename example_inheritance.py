from entity.entity import Entity
import fields.field as field
import fields.storetype as storetype
from connection.configuration import ConnectionConfiguration
from manager import Manager


class Animal(Entity):
    _table_name = 'animal'
    animal_id = field.PrimaryKey(storetype.Integer(), name='animal_id_int')


class Mammal(Animal):
    _table_name = 'mammal'
    mammal_type = field.Column(storetype.Text(max_length=30), name="mammal_type")


class Monkey(Mammal):
    _table_name = 'monkey'
    monkey_name = field.Column(storetype.Text(max_length=80))
    monkey_weight = field.Column(storetype.Float())


m = Manager()
conf = ConnectionConfiguration(user="postgres",
                               password="rajka1001",
                               database="postgres")
m.connect(conf)

monkey = Monkey()
monkey.animal_id = 1
monkey.mammal_type = 'humanoid'
monkey.monkey_name = 'Ham'
monkey.monkey_weight = 20.5


