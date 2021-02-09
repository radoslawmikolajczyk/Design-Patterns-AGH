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


class Literature(Entity):  # multiple many to many relation + inheritance
    id = field.PrimaryKey(storetype.Integer(), name='id')


class Romance(Literature):  # multiple many to many relation + inheritance
    title = field.Column(storetype.Text(max_length=30), name='title')
    author_fk = rel.ManyToMany('Author', name='author_fk')


class Fantasy(Literature):  # multiple many to many relation + inheritance
    title = field.Column(storetype.Text(max_length=30), name='title')
    author_fk = rel.ManyToMany('Author', name='author_fk')


class Author(Entity):  # multiple many to many relation + inheritance
    _table_name = 'autor'
    id = field.PrimaryKey(storetype.Integer(), name='id')
    romance_fk = rel.ManyToMany('Romance')
    fantasy_fk = rel.ManyToMany('Fantasy')
    birth_year = rel.ManyToOne('Birthday')


class Birthday(Entity):
    year = field.PrimaryKey(storetype.Integer(), name='id')


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

find = m.find_by_id(Monkey(), 1)
print("\nI found a monkey!\nMammal type:", find.mammal_type, "Name:", find.monkey_name, "Weight:", find.monkey_weight, "\n")

m.delete(monkey)

# EXAMPLE 2 ######################## simple multi insert #############################################

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

find = m.find_by_id(Match(), 1)
print("\nI found a match:", find.id, find.player_fk[0].id, find.player_fk[0].name)

m.delete(f1)
m.delete(f2)
m.delete(m1)
m.delete(m2)

# EXAMPLE 3 ######################## multiple many to many ############################################

r1 = Romance()
r2 = Romance()
fa1 = Fantasy()
fa2 = Fantasy()

a1 = Author()
a2 = Author()

b = Birthday()

r1.id = 1
r1.title = 'title1'
r1.author_fk = [a1, a2]

r2.id = 2
r2.title = 'title2'
r2.author_fk = [a1]

fa1.id = 1
fa1.title = 'title1'
fa1.author_fk = [a1, a2]

fa2.id = 2
fa2.title = 'title2'
fa2.author_fk = [a1, a2]

b.year = 1990

a1.id = 1
a1.romance_fk = [r1, r2]
a1.fantasy_fk = [fa1, fa2]
a1.birth_year = b

a2.id = 1
a2.romance_fk = [r2]
a2.fantasy_fk = [fa1, fa2]
a2.birth_year = b

m.multi_insert([a1, a2, fa1, fa2, r1, r2, b])

m.delete(a1)
m.delete(a2)
m.delete(fa1)
m.delete(fa2)
m.delete(r1)
m.delete(r2)
m.delete(b)
m.close()

