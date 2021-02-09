from typing import List

import builders.ddl as ddl
from builders.ddl import DDLConstraintAction
from builders.delete import DeleteBuilder
from builders.insert import InsertBuilder
from builders.select import SelectBuilder
from builders.update import UpdateBuilder
from connection.configuration import ConnectionConfiguration
from connection.database import DatabaseConnection
from connection.query import QueryResult
from manager.manager_helper_functions import *
from manager.scanner import Scanner
from manager.table_mapper import TableMapper


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
        self.__scanner = Scanner()
        # get all table names from all classes which inherit from Entity
        # __class_table_dict to slownik np. { <class '__main__.Address'> : _table_name, ... }
        self.__all_data, self.__class_names, self.__class_table_dict = self.__scanner.get_all_instances(Entity)
        self.__table_mapper = TableMapper(self.__class_names, self.__all_data, self)
        self.__junction_tables = []
        self.__junction_tables_names = []
        # self.__class_inheritance -- dict for all classes (without Entity) which inherit,
        # ex. { <class '__main__.Address'> : [<class '__main__.City'>, <class '__main__.Street'>]}
        self.__class_inheritance = get_all_inheritances(self.__class_names.values())
        self.__inherit_pk_all_data_modification()
        self.__insert_query = 'INSERT'
        self.__update_query = 'UPDATE'
        self.__delete_query = 'DELETE'
        self.__joined_table = 'joined_tables'
        self.__join_char = '_'
        self.__create_table = 'CREATE TABLE'
        self.__create_junction = 'CREATE JUNCTION TABLE'
        self.__not_connected_error = 'CREATE A CONNECTION TO DATABASE!'

    def __inherit_pk_all_data_modification(self):
        for i in range(len(self.__all_data)):
            name = list(self.__all_data.keys())[i]
            inherit = get_key_by_value(self.__class_table_dict, name) in self.__class_inheritance
            if inherit:
                class_key = get_key_by_value(self.__class_table_dict, name)
                inherited_classes = self.__class_inheritance[class_key]
                base_tab_name = self.__class_table_dict[inherited_classes[0]]
                pk = find_primary_key_of_table(self.__all_data, base_tab_name)
                primary_key_value = self.__all_data[base_tab_name][pk[0]]

                new_dict = dict()
                new_dict[pk[0]] = primary_key_value
                for k, v in self.__all_data[name].items():
                    if not isinstance(v, PrimaryKey):
                        new_dict[k] = v
                self.__all_data[name] = new_dict

    # create table for a given class
    def create_table(self, entity: Entity):
        table_name = get_table_name(entity)
        values_from_all_data = self.__all_data[table_name]
        data = {table_name: values_from_all_data}
        self.__create_tables_mapper(data)

    def __create_tables_mapper(self, data):
        if self.__is_connected:
            for i in range(len(data)):
                builder = ddl.DDLBuilder()
                has_primary_key = False
                name = list(data.keys())[i]
                builder.name(name)
                for key, value in data[name].items():
                    value.name = get_column_name(value, key)
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
                        foreign_key = self.__table_mapper.get_o_relation(value.other)
                        builder.field(value.name, foreign_key[2])
                        if isinstance(value, OneToOne):
                            builder.unique(value.name)  # Because it is one to one relation
                        foreign_table = get_table_name(self.__class_names.get(value.other))
                        builder.foreign_key(value.name, foreign_table, foreign_key[1],
                                            on_update=DDLConstraintAction.CASCADE,
                                            on_delete=DDLConstraintAction.CASCADE)
                    if isinstance(value, ManyToMany):
                        second_table = get_table_name(self.__class_names.get(value.other))
                        second_value = self.__table_mapper.find_many_to_many_relation_value(second_table)
                        self.__create_junction_table(name, second_table, value, second_value)

                self.__database_connection.commit(builder.build(), self.__create_table)
            for build in self.__junction_tables:
                self.__database_connection.commit(build, self.__create_junction)
        else:
            print(self.__not_connected_error)

    def create_tables(self):
        self.__create_tables_mapper(self.__all_data)

    def __create_junction_table(self, first_table, second_table, first_field_name, second_field_name):
        table_name = first_table + self.__join_char + second_table
        reversed_name = second_table + self.__join_char + first_table

        if table_name not in self.__junction_tables and reversed_name not in self.__junction_tables_names:
            first_fk = find_primary_key_of_table(self.__all_data, first_table)
            second_fk = find_primary_key_of_table(self.__all_data, second_table)
            builder = ddl.DDLBuilder()
            builder.name(table_name)
            builder.field(first_field_name.name, first_fk[2], False)
            builder.field(second_field_name.name, second_fk[2], False)
            builder.foreign_key(first_field_name.name, second_table, second_fk[1],
                                on_update=DDLConstraintAction.CASCADE,
                                on_delete=DDLConstraintAction.CASCADE)
            builder.foreign_key(second_field_name.name, first_table, first_fk[1],
                                on_update=DDLConstraintAction.CASCADE,
                                on_delete=DDLConstraintAction.CASCADE)
            builder.unique([first_field_name.name, second_field_name.name])
            self.__junction_tables_names.append(table_name)
            self.__junction_tables.append(builder.build())
            print(builder.build())
            data = dict()
            data[first_field_name.name] = first_fk[2]
            data[second_field_name.name] = second_fk[2]
            data[self.__joined_table] = [first_table, second_table]
            self.__all_data[table_name] = data

    def connect(self, conf: ConnectionConfiguration):
        self.__database_connection.configure(conf)
        self.__database_connection.connect()
        self.__is_connected = True

    def close(self):
        self.__database_connection.close()
        self.__is_connected = False

    def insert(self, entity: Entity):
        self._execute_sql_function(entity, self.__insert_query)

    def delete(self, entity: Entity):
        self._execute_sql_function(entity, self.__delete_query)

    def update(self, entity: Entity):
        self._execute_sql_function(entity, self.__update_query)

    def _execute_sql_function(self, entity: Entity, query_type: str):
        tables, primary_key = self._get_tables_and_primary_key_of_entity(entity)

        assert primary_key is not None
        primary_key_field_name, primary_key_name, primary_key_type = primary_key
        primary_key_value = getattr(entity, primary_key_field_name)
        primary_key_saved_value = entity.get_primary_key()
        entity._primary_key = primary_key_value

        for table_name in tables:

            builder = None
            if query_type is self.__insert_query:
                builder = InsertBuilder().into(table_name)
            elif query_type is self.__update_query:
                builder = UpdateBuilder().table(table_name)
            elif query_type is self.__delete_query:
                builder = DeleteBuilder().table(table_name)

            assert builder is not None

            if query_type in (self.__insert_query, self.__update_query):
                types, names, values = find_names_types_values_of_column(table_name, entity, self.__all_data, self.__class_names)
                for field_name in types.keys():
                    store_type = types[field_name]
                    column_name = names[field_name]
                    value = values[field_name]

                    # case for values that are not already assigned (NULL) or are assigned with None
                    if not isinstance(value, Column) and \
                            not isinstance(value, PrimaryKey) and \
                            not isinstance(value, Relationship) and \
                            value is not None:
                        builder.add(column_name, store_type, value)

            if query_type in (self.__update_query, self.__delete_query):
                builder.where(primary_key_name, primary_key_type, primary_key_saved_value)

            self._execute_query(builder.build(), query_type)

    def _get_tables_and_primary_key_of_entity(self, entity: Entity):
        table_name = get_table_name(entity)
        has_inheritance = get_key_by_value(self.__class_table_dict, table_name) in self.__class_inheritance
        tables = [table_name]

        if has_inheritance:
            inherited_classes = self.__class_inheritance[entity.__class__]
            for iclass in inherited_classes:
                tables.append(self.__class_table_dict[iclass])
            base_class_table_name = self.__class_table_dict[inherited_classes[0]]
            primary_key = find_primary_key_of_table(self.__all_data, base_class_table_name)
        else:
            primary_key = find_primary_key_of_table(self.__all_data, table_name)

        return tables, primary_key

    def multi_insert(self, entities: List[Entity]):
        for entity in entities:
            self.insert(entity)
        for entity in entities:
            table_name = get_table_name(entity)
            fields = self.__all_data[table_name].items()
            for field_name, field_object in fields:
                if isinstance(field_object, ManyToMany):
                    first_fk = find_primary_key_of_table(self.__all_data, table_name)
                    second_entity = self.__class_names.get(field_object.other)
                    second_table_name = get_table_name(second_entity)
                    second_fk = find_primary_key_of_table(self.__all_data, second_table_name)

                    assert first_fk is not None
                    assert second_fk is not None

                    first_fk_field_name, _, first_fk_type = first_fk
                    first_fk_value = getattr(entity, first_fk_field_name)
                    class_name = type(entity).__name__
                    first_fk_name = find_name_of_first_fk_column(self.__all_data, class_name, second_table_name)

                    second_fk_field_name, _, second_fk_type = second_fk
                    second_fk_objects = getattr(entity, field_name)
                    second_fk_values = find_foreign_key_values_in_m2m(second_fk_objects, second_fk_field_name)
                    second_fk_name = get_column_name(field_object, field_name)

                    junction_table_name = self._find_name_of_junction_table(table_name, second_table_name)

                    for second_value in second_fk_values:
                        builder = InsertBuilder().into(junction_table_name)
                        builder.add(first_fk_name, first_fk_type, first_fk_value)
                        builder.add(second_fk_name, second_fk_type, second_value)
                        self._execute_query(builder.build(), self.__insert_query)

    def _find_name_of_junction_table(self, first: str, second: str):
        junction_table_name = first + self.__join_char + second
        if junction_table_name not in self.__junction_tables_names:
            junction_table_name = second + self.__join_char + first

        return junction_table_name

    def find_by_id(self, model: Entity, id, many_keys=False):
        table_name = get_table_name(model)
        id_field_name, _, _ = find_primary_key_of_table(self.__all_data, table_name)
        result = self.find_by(model, id_field_name, id, many_keys)
        try:
            return result[0]
        except IndexError:  # Didn't find anything
            return None

    def find_by(self, model: Entity, key_name: str, key_value, many_keys=False):
        table_name = get_table_name(model)
        types, names, _ = find_names_types_values_of_column(table_name, None, self.__all_data, self.__class_names)
        builder = self.__build_regular(table_name, types, names, key_name, key_value)

        junction_data = None
        if check_got_many_relation(table_name, self.__junction_tables_names):
            junction_data = self.__build_many_relation(table_name, builder)

        model_inherits = (type(model) in self.__class_inheritance.keys())
        if model_inherits:
            self.__build_inheritance(model, builder)

        query, fields = builder.build()
        return self.__select(model, query, many_keys, junction_data)

    def __build_regular(self, table_name, types, names, key_name, key_value):
        builder = SelectBuilder().table(table_name)
        for field_name in types.keys():
            builder.add(table_name, names[field_name])
        builder.where(names[key_name], types[key_name], key_value)
        return builder

    def __build_many_relation(self, table_name, builder):
        junction_data = extract_junction_data(table_name, self.__junction_tables_names, self.__junction_tables)
        other = find_many_to_many_other_field(table_name, self.__all_data)
        _, key_name, _ = find_primary_key_of_table(self.__all_data, table_name)

        for index, data in enumerate(junction_data):
            junction_table_name, junction_key, _ = data
            builder.join(junction_table_name, junction_key, table_name, key_name)

            builder.add(junction_table_name, other[index].name)
        return junction_data

    def __build_inheritance(self, model, builder):
        parents = self.__class_inheritance[type(model)]
        table_name = get_table_name(model)
        _, model_pk, _ = find_primary_key_of_table(self.__all_data, table_name)

        for parent_class in parents:
            parent_name = get_table_name(parent_class)
            _, parent_pk, _ = find_primary_key_of_table(self.__all_data, parent_name)
            builder.join(parent_name, parent_pk, table_name, parent_pk)
            self.__add_parent_columns(builder, parent_name, model_pk)

    def __add_parent_columns(self, builder, table_name, child_pk_name):
        _, names, _ = find_names_types_values_of_column(table_name, None, self.__all_data, self.__class_names)
        for name in names.values():
            if name != child_pk_name:
                builder.add(table_name, name)

    def __select(self, model: Entity, query: str, many_keys=False, junction_data=None) -> list:
        if self.__is_connected:
            names = get_field_names_dict(model, self.__all_data, self.__class_names, self.__class_inheritance)
            query_result = self.__select_raw(query)
            self.__table_mapper.inheritance = self.__class_inheritance
            result = self.__table_mapper.map_result_fields(model, names, query_result, many_keys, junction_data)
            return result
        else:
            print("CREATE A CONNECTION TO DATABASE!")
            return []

    def __select_raw(self, query: str) -> QueryResult:
        if self.__is_connected:
            query_result = self.__database_connection.execute(query)
            return query_result
        else:
            print("CREATE A CONNECTION TO DATABASE!")
            return QueryResult(['None'])

    def _execute_query(self, query: str, query_type: str = 'QUERY'):
        if self.__is_connected:
            self.__database_connection.commit(query, query_type)
        else:
            print(self.__not_connected_error)
