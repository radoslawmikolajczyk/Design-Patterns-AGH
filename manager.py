from builders.delete import DeleteBuilder
from builders.insert import InsertBuilder
from builders.update import UpdateBuilder
from connection.configuration import ConnectionConfiguration
from connection.database import DatabaseConnection
from entity.entity import Entity
import gc
import inspect
from collections import defaultdict
from fields import field
from fields import storetype
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
        self.__tables, self.__all_data = self.__get_all_instances(Entity)
        print(self.__all_data)

    def create_tables(self):
        # Do rozwiniecia o nowe rzeczy w ddl builder unique, not null etc..
        if self.__is_connected:
            for i in range(len(self.__all_data)):
                builder = ddl.DDLBuilder()
                has_primary_key = False
                name = self.__all_data[i].get('_table_name')
                builder.name(name)
                for key, value in self.__all_data[i].items():
                    if type(value).__name__ == 'Column':
                        builder.field(value.name, value.type)
                        if not has_primary_key:
                            builder.primary_key(value.name)
                            has_primary_key = True
                    if type(value).__name__ == 'PrimaryKey':
                        builder.field(value.name, value.type)
                        builder.primary_key(value.name)
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

    def __get_all_instances(self, cls):
        table_names = []
        tables = []
        for subclass in cls.__subclasses__():
            for obj in gc.get_objects():
                entire_table = {}
                if isinstance(obj, subclass):
                    attr = inspect.getmembers(obj, lambda a: not (inspect.isroutine(a)))
                    attr_list = [a for a in attr if not (a[0].startswith('__') and a[0].endswith('__'))]
                    dictionary = defaultdict(list)
                    for i, j in attr_list:
                        dictionary[i].append(j)
                    dictionary = dict(dictionary)

                    for key, value in dictionary.items():

                        # print(key, " -> ", type(value[0]).__name__)
                        if key == '_table_name':
                            entire_table[key] = value[0]
                        else:
                            entire_table[key] = value[0]
                            '''column_params = value[0].__dict__
                            column_dict = {}
                            for k, v in column_params.items():
                                if k == 'type':
                                    type_dict = {key: value[0]}
                                    type_data = {'StoreType': v.__class__.__name__}
                                    for k1, v1 in v.__dict__.items():
                                        type_data[k1] = v1
                                    type_dict['type_params'] = type_data
                                    column_dict[k] = type_dict
                                else:
                                    column_dict[k] = v
                            entire_table[key] = column_dict
                            #print(column_dict)'''
                    tables.append(entire_table)

                    table_names.append(dictionary.get('_table_name')[0])
        return table_names, tables
