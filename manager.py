from builders.insert import InsertBuilder
from connection.configuration import ConnectionConfiguration
from connection.database import DatabaseConnection
from entity.entity import Entity
import gc
import inspect
from collections import defaultdict
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
        self.__tables = self.__get_all_instances(Entity)
        print(self.__tables)
        self.__fields = []

    def connect(self, conf: ConnectionConfiguration):
        self.__database_connection.configure(conf)
        self.__database_connection.connect()

    def close(self):
        self.__database_connection.close()

    def insert(self, entity: Entity):
        # TODO: - find the exact name of the table
        #       - find the columns (name, type) of the table
        table_name = str(type(entity))
        columns = dict()  # Dict[str, StoreType]
        builder = InsertBuilder().into(table_name)
        for column_name in columns:
            store_type = columns[column_name]
            value = getattr(entity, column_name)
            builder.add(column_name, store_type, value)
        query = builder.build()
        self.__database_connection.execute(query)

    def delete(self, entity: Entity):
        pass

    def update(self, entity: Entity):
        pass

    def findById(self, model, id):
        pass

    def select(self, model, query: str) -> list:
        return []

    def __get_all_instances(self, cls):
        table_names = []
        for subclass in cls.__subclasses__():
            for obj in gc.get_objects():
                if isinstance(obj, subclass):
                    attr = inspect.getmembers(obj, lambda a: not (inspect.isroutine(a)))
                    print([a for a in attr if not (a[0].startswith('__') and a[0].endswith('__'))])
                    attr_list = [a for a in attr if not (a[0].startswith('__') and a[0].endswith('__'))]
                    dictionary = defaultdict(list)
                    for i, j in attr_list:
                        dictionary[i].append(j)
                    print(dict(dictionary))
                    table_names.append(dict(dictionary).get('_table_name')[0])
        return table_names


class Person(Entity):
    first_name = field.Column(storetype.Text(max_length=30), name="first_name")

    def __init__(self, name):
        super().__init__(name)


class Address(Entity):
    def __init__(self, name):
        super().__init__(name)


class City(Address):
    def __init__(self, name):
        super().__init__(name)


p = Person('people')
print(p.first_name.type.max_length)
p1 = Address('ppp')
p2 = City('ososo')
m = Manager()
