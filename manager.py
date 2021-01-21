from builders.delete import DeleteBuilder
from builders.insert import InsertBuilder
from builders.update import UpdateBuilder
from builders.select import SelectBuilder

from connection.configuration import ConnectionConfiguration
from connection.database import DatabaseConnection
from entity.entity import Entity
from collections import defaultdict
from connection.query import QueryResult
from fields.relationship import OneToOne, ManyToOne, ManyToMany, Relationship

from fields.field import Column, PrimaryKey
import builders.ddl as ddl

from copy import deepcopy


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
        self.__all_data, self.__class_names = self.__get_all_instances(Entity)
        self.__junction_tables = []
        self.__junction_tables_names = []

    def create_tables(self):
        if self.__is_connected:
            for i in range(len(self.__all_data)):
                builder = ddl.DDLBuilder()
                has_primary_key = False
                name = list(self.__all_data.keys())[i]
                builder.name(name)
                for key, value in self.__all_data[name].items():
                    if isinstance(value, PrimaryKey):
                        builder.field(value.name, value.type, False)
                        builder.primary_key(value.name)
                        has_primary_key = True
                    if isinstance(value, Column):
                        builder.field(value.name, value.type, value.nullable)
                        if not has_primary_key:
                            builder.primary_key(value.name)
                            has_primary_key = True
                        if value.unique:
                            builder.unique(value.name)
                    if isinstance(value, OneToOne) or isinstance(value, ManyToOne):
                        foreign_key = self.__get_o_relation(value.other)
                        builder.field(value.name, foreign_key[2])
                        if isinstance(value, OneToOne):
                            builder.unique(value.name)  # Because it is one to one relation
                        foreign_table = self._get_table_name(self.__class_names.get(value.other))
                        builder.foreign_key(value.name, foreign_table, foreign_key[1])
                    if isinstance(value, ManyToMany):
                        second_table = self._get_table_name(self.__class_names.get(value.other))
                        second_value = self.__find_many_to_many_relation_value(second_table)
                        self.__create_junction_table(name, second_table, value, second_value)

                self.__database_connection.commit(builder.build(), 'CREATE TABLE')
            for build in self.__junction_tables:
                self.__database_connection.commit(build, 'CREATE JUNCTION TABLE')
            print(self.__all_data)
            print(self.__class_names)
            print(self.__junction_tables)
            print(self.__junction_tables_names)
        else:
            print("CREATE A CONNECTION TO DATABASE!")

    # OneToOne and OneToMany relations
    def __get_o_relation(self, entity_name):
        entity = self.__class_names.get(entity_name)
        table_name = self._get_table_name(entity)
        return self._find_primary_key_of_table(table_name)

    def __find_many_to_many_relation_value(self, table_name):
        for key, value in self.__all_data[table_name].items():
            if isinstance(value, ManyToMany):
                return value

    def __create_junction_table(self, first_table, second_table, first_field_name, second_field_name):
        table_name = first_table + "_" + second_table
        reversed_name = second_table + "_" + first_table

        if table_name not in self.__junction_tables and reversed_name not in self.__junction_tables_names:
            first_fk = self._find_primary_key_of_table(first_table)
            second_fk = self._find_primary_key_of_table(second_table)

            builder = ddl.DDLBuilder()
            builder.name(table_name)
            builder.field(first_field_name.name, first_fk[2], False)
            builder.field(second_field_name.name, second_fk[2], False)
            builder.foreign_key(first_field_name.name, second_table, second_fk[1])
            builder.foreign_key(second_field_name.name, first_table, first_fk[1])

            self.__junction_tables_names.append(table_name)
            self.__junction_tables.append(builder.build())
            print(builder.build())
            data = dict()
            data[first_field_name.name] = first_fk[2]
            data[second_field_name.name] = second_fk[2]
            data['joined_tables'] = [first_table, second_table]
            self.__all_data[table_name] = data

    def connect(self, conf: ConnectionConfiguration):
        self.__database_connection.configure(conf)
        self.__database_connection.connect()
        self.__is_connected = True

    def close(self):
        self.__database_connection.close()
        self.__is_connected = False

    def insert(self, entity: Entity):
        table_name = self._get_table_name(entity)
        types, names = self._find_names_and_types_of_columns(table_name)
        builder = InsertBuilder().into(table_name)
        for field_name in types.keys():
            store_type = types[field_name]
            column_name = names[field_name]
            value = getattr(entity, field_name)
            if not isinstance(value, Column) and \
               not isinstance(value, PrimaryKey) and \
               not isinstance(value, Relationship):
                builder.add(column_name, store_type, value)

        self._execute_query(builder.build(), 'INSERT')

    def delete(self, entity: Entity):
        table_name = self._get_table_name(entity)
        primary_key = self._find_primary_key_of_table(table_name)
        assert primary_key is not None
        primary_key_field_name, primary_key_name, primary_key_type = primary_key
        primary_key_value = getattr(entity, primary_key_field_name)

        builder = DeleteBuilder().table(table_name)
        builder.where(primary_key_name, primary_key_type, primary_key_value)

        self._execute_query(builder.build(), 'DELETE')

    def update(self, entity: Entity):
        table_name = self._get_table_name(entity)
        types, names = self._find_names_and_types_of_columns(table_name)
        primary_key = self._find_primary_key_of_table(table_name)
        assert primary_key is not None
        primary_key_field_name, primary_key_name, primary_key_type = primary_key
        primary_key_value = getattr(entity, primary_key_field_name)

        builder = UpdateBuilder().table(table_name)
        for field_name in types.keys():
            store_type = types[field_name]
            column_name = names[field_name]
            value = getattr(entity, field_name)
            builder.add(column_name, store_type, value)
        builder.where(primary_key_name, primary_key_type, primary_key_value)

        self._execute_query(builder.build(), 'UPDATE')

    def find_by_id(self, model: Entity, id):
        table_name = self._get_table_name(model)
        id_name, _, id_type = self._find_primary_key_of_table(table_name)
        _, names = self._find_names_and_types_of_columns(table_name)

        builder = SelectBuilder().table(table_name)
        for field_name in names.keys():
            column_name = names[field_name]
            builder.add(table_name, column_name)
        builder.where(id_name, id_type, id)
        query, fields = builder.build()
        try:
            return self.select(model, query)[0]
        except IndexError:  # Didn't find anything
            return None

    def select(self, model: Entity, query: str) -> list:
        if self.__is_connected:
            table_name = self._get_table_name(model)
            _, names = self._find_names_and_types_of_columns(table_name)
            query_result = self.__database_connection.execute(query)
            result = self.__map_result_fields(model, names.keys(), query_result)
            return result
        else:
            print("CREATE A CONNECTION TO DATABASE!")
            return []

    def __all_subclasses(self, cls):
        all_subclasses = []

        for subclass in cls.__subclasses__():
            all_subclasses.append(subclass)
            all_subclasses.extend(self.__all_subclasses(subclass))

        return all_subclasses

    def __get_all_instances(self, cls):
        cls = self.__all_subclasses(cls)
        tables = dict()
        class_names = dict()
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

            tables[t_name] = new_dict
            class_names[subclass.__name__] = subclass
        return tables, class_names

    def __map_result_fields(self, model: Entity, field_names: [str], query_result: QueryResult):
        records = query_result.get_query()
        mapped = [deepcopy(model) for _ in records]
        for i, record in enumerate(records):
            for j, field in enumerate(record):
                setattr(mapped[i], list(field_names)[j], field)
        return mapped

    def _get_table_name(self, entity: Entity):
        if entity._table_name is not "":
            table_name = entity._table_name
        else:
            if isinstance(entity, Entity):
                entity = type(entity)
            table_name = entity.__name__.lower()
        return table_name

    def _find_names_and_types_of_columns(self, table_name):
        fields = self.__all_data[table_name].items()
        types = dict()  # [field_name : column_type]
        names = dict()  # [field_name : column_name]

        for field_name, field_object in fields:
            if isinstance(field_object, Column) or isinstance(field_object, PrimaryKey):
                types[field_name] = field_object.type
                names[field_name] = field_object.name
            else:  # object is a relation
                # we need to find a primary key of the table to which relationship is
                # in order to find the type of the field

                entity = self.__class_names.get(field_object.other)
                table_name = self._get_table_name(entity)
                primary_key = self._find_primary_key_of_table(table_name)
                assert primary_key is not None
                _, _, primary_key_type = primary_key

                types[field_name] = primary_key_type
                names[field_name] = field_object.name

        return types, names

    def _find_primary_key_of_table(self, table_name):
        fields = self.__all_data[table_name].items()

        for field_name, field_object in fields:
            if isinstance(field_object, PrimaryKey):
                primary_key = [field_name, field_object.name, field_object.type]
                return primary_key

        # if we don't find the primary key field, the primary key is the first column
        for field_name, field_object in fields:
            if isinstance(field_object, Column):
                primary_key = [field_name, field_object.name, field_object.type]
                return primary_key

        return None

    def _execute_query(self, query: str, query_type: str = 'QUERY'):
        if self.__is_connected:
            self.__database_connection.commit(query, query_type)
        else:
            print("CREATE A CONNECTION TO DATABASE!")
