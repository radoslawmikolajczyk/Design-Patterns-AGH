from connection.configuration import ConnectionConfiguration
from connection.database import DatabaseConnection
from entity.entity import Entity
import gc
from fields import field
from fields import storetype


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class Manager(metaclass=SingletonMeta):

    def __init__(self):
        self.__database_connection = DatabaseConnection()
        # get all table names from all classes which inherit from Entity
        self.__tables = self.__get_all_subclasses(Entity)
        print(self.__tables)
        self.__fields = []

    def connect(self, conf: ConnectionConfiguration):
        self.__database_connection.configure(conf)
        self.__database_connection.connect()

    def close(self):
        self.__database_connection.close()

    def insert(self, entity: Entity):
        pass

    def delete(self, entity: Entity):
        pass

    def update(self, entity: Entity):
        pass

    def findById(self, model, id):
        pass

    def select(self, model, query: str) -> list:
        return []

    def __get_all_subclasses(self, cls):
        all_subclasses = []
        for subclass in cls.__subclasses__():
            for obj in gc.get_objects():
                if isinstance(obj, subclass):
                    all_subclasses.append(obj.get_table_name())
        return all_subclasses

class Person(Entity):
    first_name = field.Column(storetype.Text(max_length=30), name="first_name")
    def __init__(self, name):
        super().__init__(name)

class Pig(Entity):
    def __init__(self, name):
        super().__init__(name)

class Pig2(Pig):
    def __init__(self, name):
        super().__init__(name)

p = Person('people')
print(p.first_name.name)
p1 = Pig('ppp')
p2 = Pig2('ososo')
m = Manager()
