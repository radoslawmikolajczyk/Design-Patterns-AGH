from builders.delete import DeleteBuilder
from builders.insert import InsertBuilder
from builders.update import UpdateBuilder
from connection.configuration import ConnectionConfiguration
from connection.database import DatabaseConnection
from entity.entity import Entity
from collections import defaultdict
from fields.storetype import StoreType
import builders.ddl as ddl


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class Manager(metaclass=SingletonMeta):

    def __init__(self):
        self.__is_connected = False
        self.__database_connection = DatabaseConnection()
        # get all table names from all classes which inherit from Entity
        self.__all_data = self.__get_all_instances(Entity)
        print(self.__all_data)

    def create_tables(self):
        # TODO: relacje, foreign_key itd...
        if self.__is_connected:
            for i in range(len(self.__all_data)):
                builder = ddl.DDLBuilder()
                has_primary_key = False
                name = list(self.__all_data[i])[0]
                builder.name(name)
                for key, value in list(self.__all_data[i].values())[0].items():
                    if type(value).__name__ == 'PrimaryKey':
                        builder.field(value.name, value.type, False)
                        builder.primary_key(value.name)
                        has_primary_key = True
                    if type(value).__name__ == 'Column':
                        builder.field(value.name, value.type, value.nullable)
                        if not has_primary_key:
                            builder.primary_key(value.name)
                            has_primary_key = True
                        if value.unique:
                            builder.unique(value.name)

                self.__database_connection.commit(builder.build(), 'CREATE TABLE')
        else:
            print("CREATE A CONNECTION TO DATABASE!")
        self.close()

    def connect(self, conf: ConnectionConfiguration):
        self.__database_connection.configure(conf)
        self.__database_connection.connect()
        self.__is_connected = True

    def close(self):
        self.__database_connection.close()
        self.__is_connected = False

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
        self.__database_connection.commit(query)

    def delete(self, entity: Entity):
        # TODO: - find the exact name of the table
        #       - find the primary key (name, type) of the table
        table_name = str(type(entity))
        primary_key = ('', StoreType())  # Tuple[str, str]
        primary_key_name, store_type = primary_key
        value = getattr(entity, primary_key_name)

        builder = DeleteBuilder().table(table_name)
        builder.where(primary_key_name, store_type, value)

        query = builder.build()
        self.__database_connection.commit(query)

    def update(self, entity: Entity):
        # TODO: - find the exact name of the table
        #       - find the columns (name, type) of the table
        #       - find the primary key (name, type) of the table
        table_name = str(type(entity))
        columns = dict()  # Dict[str, StoreType]

        builder = UpdateBuilder().table(table_name)
        for column_name in columns:
            store_type = columns[column_name]
            value = getattr(entity, column_name)
            builder.add(column_name, store_type, value)

        primary_key = ('', StoreType())  # Tuple[str, str]
        primary_key_name, store_type = primary_key
        value = getattr(entity, primary_key_name)
        builder.where(primary_key_name, store_type, value)

        query = builder.build()
        self.__database_connection.commit(query)

    def find_by_id(self, model, id):
        pass

    def select(self, model, query: str) -> list:
        return []

    def __all_subclasses(self, cls):
        return set(cls.__subclasses__()).union(
            [s for c in cls.__subclasses__() for s in self.__all_subclasses(c)])

    def __get_all_instances(self, cls):
        cls = self.__all_subclasses(cls)
        tables = []
        for subclass in cls:
            values = subclass.__dict__
            dictionary = defaultdict(list)
            for a, b in values.items():
                if not (a.startswith('__') and a.endswith('__')):
                    dictionary[a].append(b)
            dictionary = dict(dictionary)
            new_dict = {}
            t_name = subclass.__name__.lower()
            for key, value in dictionary.items():
                if not key == '_table_name':
                    new_dict[key] = value[0]
                else:
                    t_name = dictionary.get('_table_name')[0]

            tables.append({t_name: new_dict})
        return tables
