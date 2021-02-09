from entity.entity import Entity
import fields.field as field
import fields.storetype as storetype
import fields.relationship as rel
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


class Player(Entity):
    id = field.PrimaryKey(storetype.Integer(), name='player_id')


class Footballer(Player):
    name = field.Column(storetype.Text(max_length=80))
    match_fk = rel.ManyToMany('Match', name='match')


class Match(Entity):
    id = field.PrimaryKey(storetype.Integer(), name='match_id')
    player_fk = rel.ManyToMany('Footballer', name='player')

m = Manager()
conf = ConnectionConfiguration(user="postgres",
                               password="rajka1001",
                               database="postgres")
m.connect(conf)
m.create_tables()

# EXAMPLE 1 ######################## simple insert, update, delete ##################################

monkey = Monkey()
monkey.animal_id = 1
monkey.mammal_type = 'humanoid'
monkey.monkey_name = 'Ham'
monkey.monkey_weight = 20.5

m.insert(monkey)
monkey.monkey_weight = 30.9
m.update(monkey)
m.delete(monkey)

# EXAMPLE 2 ######################## simple multi insert ##################################

f1 = Footballer()
f2 = Footballer()
m1 = Match()
m2 = Match()

f1.id = 1
f1.name = "nfname1"
f1.match_fk = [m1, m2]

f2.id = 2
f2.name = "fname2"
f2.match_fk = [m1, m2]

m1.id = 1
m1.player_fk = [f1, f2]

m2.id = 2
m2.player_fk = [f1, f2]

m.multi_insert([f1, f2, m1, m2])
m.delete(f1)
m.delete(f2)
m.delete(m1)
m.delete(m2)

m.close()
