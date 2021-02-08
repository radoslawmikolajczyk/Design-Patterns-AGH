from typing import List

from builders.delete import DeleteBuilder
from builders.insert import InsertBuilder
from builders.update import UpdateBuilder
from builders.select import SelectBuilder
from builders.ddl import DDLConstraintAction

from connection.configuration import ConnectionConfiguration
from connection.database import DatabaseConnection
from entity.entity import Entity
from collections import defaultdict
from connection.query import QueryResult
from fields.relationship import OneToOne, ManyToOne, ManyToMany, Relationship

from fields.field import Column, PrimaryKey, Field
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
        # __class_table_dict to slownik np. { <class '__main__.Address'> : _table_name, ... }
        self.__all_data, self.__class_names, self.__class_table_dict = self.__get_all_instances(Entity)
        self.__junction_tables = []
        self.__junction_tables_names = []
        # self.__class_inheritance -- dict for all classes (without Entity) which inherit,
        # ex. { <class '__main__.Address'> : [<class '__main__.City'>, <class '__main__.Street'>]}
        self.__class_inheritance = self.__get_all_inheritances(self.__class_names.values())
        self.__inherit_pk_all_data_modification()
        # print(self.__all_data)
        # print(self.__class_names)
        # print(self.__class_table_dict)
        # print(self.__junction_tables)
        # print(self.__junction_tables_names)
        # print(self.__class_inheritance)

    def __inherit_pk_all_data_modification(self):
        for i in range(len(self.__all_data)):
            name = list(self.__all_data.keys())[i]
            inherit = self.__get_table_name_by_class(self.__class_table_dict, name) in self.__class_inheritance
            if inherit:
                class_key = self.__get_table_name_by_class(self.__class_table_dict, name)
                inherited_classes = self.__class_inheritance[class_key]
                base_tab_name = self.__class_table_dict[inherited_classes[0]]
                pk = self._find_primary_key_of_table(base_tab_name)
                primary_key_value = self.__all_data[base_tab_name][pk[0]]

                new_dict = dict()
                new_dict[pk[0]] = primary_key_value
                for k, v in self.__all_data[name].items():
                    if not isinstance(v, PrimaryKey):
                        new_dict[k] = v
                self.__all_data[name] = new_dict

    # create table for a given class
    def create_table(self, entity: Entity):
        table_name = self._get_table_name(entity)
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
                    value.name = self.__get_column_name(value, key)
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
                        builder.foreign_key(value.name, foreign_table, foreign_key[1],
                                            on_update=DDLConstraintAction.CASCADE,
                                            on_delete=DDLConstraintAction.CASCADE)
                    if isinstance(value, ManyToMany):
                        second_table = self._get_table_name(self.__class_names.get(value.other))
                        second_value = self.__find_many_to_many_relation_value(second_table)
                        self.__create_junction_table(name, second_table, value, second_value)

                self.__database_connection.commit(builder.build(), 'CREATE TABLE')
            for build in self.__junction_tables:
                self.__database_connection.commit(build, 'CREATE JUNCTION TABLE')
        else:
            print("CREATE A CONNECTION TO DATABASE!")

    def create_tables(self):
        self.__create_tables_mapper(self.__all_data)

    def __get_table_name_by_class(self, dictionary, table_name):
        items_list = dictionary.items()
        for item in items_list:
            if item[1] == table_name:
                return item[0]

    # OneToOne and OneToMany relations
    def __get_o_relation(self, entity_name):
        entity = self.__class_names.get(entity_name)
        table_name = self._get_table_name(entity)
        return self._find_primary_key_of_table(table_name)

    def __find_many_to_many_relation_value(self, table_name):
        for key, value in self.__all_data[table_name].items():
            if isinstance(value, ManyToMany):
                return value

    def __get_column_name(self, field, field_name):
        if field.name == Field.default_name:
            return field_name
        else:
            return field.name

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
            data['joined_tables'] = [first_table, second_table]
            self.__all_data[table_name] = data

    def connect(self, conf: ConnectionConfiguration):
        self.__database_connection.configure(conf)
        self.__database_connection.connect()
        self.__is_connected = True

    def close(self):
        self.__database_connection.close()
        self.__is_connected = False

    def multi_insert(self, entities: List[Entity]):
        for entity in entities:
            self.insert(entity)
        for entity in entities:
            table_name = self._get_table_name(entity)
            fields = self.__all_data[table_name].items()
            for field_name, field_object in fields:
                if isinstance(field_object, ManyToMany):
                    first_fk = self._find_primary_key_of_table(table_name)
                    second_entity = self.__class_names.get(field_object.other)
                    second_table_name = self._get_table_name(second_entity)
                    second_fk = self._find_primary_key_of_table(second_table_name)

                    assert first_fk is not None
                    assert second_fk is not None

                    first_fk_field_name, _, first_fk_type = first_fk
                    first_fk_value = getattr(entity, first_fk_field_name)
                    class_name = type(entity).__name__
                    first_fk_name = self._find_name_of_first_fk_column(class_name, second_table_name)

                    second_fk_field_name, _, second_fk_type = second_fk
                    second_fk_objects = getattr(entity, field_name)
                    second_fk_values = self._find_foreign_key_values_in_m2m(second_fk_objects, second_fk_field_name)
                    second_fk_name = self.__get_column_name(field_object, field_name)

                    junction_table_name = self._find_name_of_junction_table(table_name, second_table_name)

                    for second_value in second_fk_values:
                        builder = InsertBuilder().into(junction_table_name)
                        builder.add(first_fk_name, first_fk_type, first_fk_value)
                        builder.add(second_fk_name, second_fk_type, second_value)
                        self._execute_query(builder.build(), 'INSERT')

    def _find_foreign_key_values_in_m2m(self, objects, fk_field_name):
        second_fk_values = []
        if not isinstance(objects, ManyToMany):
            for relation_object in objects:
                value = getattr(relation_object, fk_field_name)
                second_fk_values.append(value)
        return second_fk_values

    def _find_name_of_first_fk_column(self, class_name, second_table_name):
        first_fk_name = None
        second_fields = self.__all_data[second_table_name].items()
        for s_field_name, s_field_object in second_fields:
            if isinstance(s_field_object, ManyToMany) and s_field_object.other == class_name:
                first_fk_name = self.__get_column_name(s_field_object, s_field_name)

        assert first_fk_name is not None
        return first_fk_name

    def _find_name_of_junction_table(self, first: str, second: str):
        junction_table_name = first + '_' + second
        if junction_table_name not in self.__junction_tables_names:
            junction_table_name = second + '_' + first

        return junction_table_name

    def _execute_sql_function(self, entity: Entity, query_type: str):
        table_name = self._get_table_name(entity)
        has_inheritance = self.__get_table_name_by_class(self.__class_table_dict, table_name) in self.__class_inheritance
        tables = [table_name]

        if has_inheritance:
            inherited_classes = self.__class_inheritance[entity.__class__]
            for iclass in inherited_classes:
                tables.append(self.__class_table_dict[iclass])
            base_class_table_name = self.__class_table_dict[inherited_classes[0]]
            primary_key = self._find_primary_key_of_table(base_class_table_name)
        else:
            primary_key = self._find_primary_key_of_table(table_name)

        assert primary_key is not None
        primary_key_field_name, primary_key_name, primary_key_type = primary_key
        primary_key_value = getattr(entity, primary_key_field_name)
        primary_key_saved_value = entity.get_primary_key()
        entity._primary_key = primary_key_value

        builder = None

        for table_name in tables:

            if query_type is 'INSERT':
                builder = InsertBuilder().into(table_name)
            elif query_type is 'UPDATE':
                builder = UpdateBuilder().table(table_name)
            elif query_type is 'DELETE':
                builder = DeleteBuilder().table(table_name)

            assert builder is not None

            if query_type in ('INSERT', 'UPDATE'):
                    types, names, values = self._find_names_types_values_of_column(table_name, entity)
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

            if query_type in ('UPDATE', 'DELETE'):
                # print(type(primary_key_name), type(primary_key_type), type(primary_key_saved_value))
                builder.where(primary_key_name, primary_key_type, primary_key_saved_value)

            self._execute_query(builder.build(), query_type)
            builder = None

    def insert(self, entity: Entity):
        self._execute_sql_function(entity, 'INSERT')

    def delete(self, entity: Entity):
        self._execute_sql_function(entity, 'DELETE')

    def update(self, entity: Entity):
        self._execute_sql_function(entity, 'UPDATE')

    def find_by_id(self, model: Entity, id):
        table_name = self._get_table_name(model)
        id_field_name, _, _ = self._find_primary_key_of_table(table_name)
        result = self.find_by(model, id_field_name, id)
        try:
            return result[0]
        except IndexError:  # Didn't find anything
            return None

    def find_by(self, model: Entity, key_name: str, key_value):
        table_name = self._get_table_name(model)
        types, names, _ = self._find_names_types_values_of_column(table_name, None)
        id_field_name, _, _ = self._find_primary_key_of_table(table_name)
        builder = SelectBuilder().table(table_name)
        for field_name in types.keys():
            builder.add(table_name, names[field_name])

        junction_data = self._get_junction_data(table_name)
        other = self.__find_many_to_many_other_fk_name(table_name)

        for index, data in enumerate(junction_data):
            junction_table_name, junction_key = data
            builder.join(junction_table_name, junction_key, table_name, id_field_name)

            builder.add(junction_table_name, other[index].name)

        builder.where(names[key_name], types[key_name], key_value)
        query, fields = builder.build()
        return self.select(model, query)

    def _get_junction_data(self, table_name: str):
        junction_data = []
        for index, junction_table_name in enumerate(self.__junction_tables_names):
            if table_name in junction_table_name:
                junction_query = self.__junction_tables[index]
                junction_query = junction_query.split(" ")
                for part_index, part in enumerate(junction_query):
                    if 'KEY' in part and table_name in junction_query[part_index + 2]:
                        junction_key = part.split('"')[1]
                        junction_data.append((junction_table_name, junction_key))
        return junction_data

    def __find_many_to_many_other_fk_name(self, table_name):
        names = []
        for key, value in self.__all_data[table_name].items():
            if isinstance(value, ManyToMany):
                names.append(value)
        return names

    def select(self, model: Entity, query: str) -> list:
        if self.__is_connected:
            table_name = self._get_table_name(model)
            _, names, _ = self._find_names_types_values_of_column(table_name, None)
            query_result = self.__database_connection.execute(query)
            result = self.__map_result_fields(model, names.keys(), query_result)
            return result
        else:
            print("CREATE A CONNECTION TO DATABASE!")
            return []

    def __map_result_fields(self, model: Entity, field_names: [str], query_result: QueryResult):
        table_name = self._get_table_name(model)
        pk_field_name, pk_column_name, pk_type = self._find_primary_key_of_table(table_name)
        aggregate = {}
        records = query_result.get_query()
        mapped = [deepcopy(model) for _ in records]

        pk_index = list(field_names).index(pk_field_name)

        for i, record in enumerate(records):
            for j, field in enumerate(record):
                setattr(mapped[i], list(field_names)[j], field)
                record_pk = record[pk_index]
                if record_pk in aggregate:
                    in_dict = getattr(aggregate[record_pk], list(field_names)[j])
                    if self._is_column(in_dict):
                        # this also changes the object in mapped list, because of the way we
                        # added this item to the dict
                        setattr(aggregate[record_pk], list(field_names)[j], field)
                    elif in_dict != field and not isinstance(in_dict, list):
                        setattr(aggregate[record_pk], list(field_names)[j], [in_dict, field])
                    elif in_dict != field and isinstance(in_dict, list) and field not in in_dict:
                        new = in_dict
                        new.append(field)
                        setattr(aggregate[record_pk], list(field_names)[j], new)
                else:
                    # if we add the mapped object to dict this way we essentialy have a dict like
                    # {primary_key_value: pointer_to_object_in_mapped_list}
                    mapped[i]._primary_key = record_pk
                    aggregate[record_pk] = mapped[i]

        return list(aggregate.values())

    def _is_column(self, field_value: str):
        return isinstance(field_value, Column) or isinstance(field_value, PrimaryKey) or \
               isinstance(field_value, ManyToOne) or isinstance(field_value, OneToOne) or \
               isinstance(field_value, ManyToMany)

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
        class_tables = dict()
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
            class_tables[subclass] = t_name
        return tables, class_names, class_tables

    def _get_table_name(self, entity: Entity):
        if entity._table_name is not "":
            table_name = entity._table_name
        else:
            if isinstance(entity, Entity):
                entity = type(entity)
            table_name = entity.__name__.lower()
        return table_name

    def _find_names_types_values_of_column(self, table_name, entity):
        fields = self.__all_data[table_name].items()
        types = dict()  # [field_name : column_type]
        names = dict()  # [field_name : column_name]
        values = dict() # [field_name : value]

        for field_name, field_object in fields:
            if isinstance(field_object, Column) or isinstance(field_object, PrimaryKey):
                types[field_name] = field_object.type
                if entity is not None:
                    values[field_name] = getattr(entity, field_name)
            elif isinstance(field_object, ManyToOne) or isinstance(field_object, OneToOne):
                primary_key = self._find_type_of_primary_key_of_relation(field_object.other)
                assert primary_key is not None
                primary_key_field_name, primary_key_name, primary_key_type = primary_key
                types[field_name] = primary_key_type
                if entity is not None:
                    relation_object = getattr(entity, field_name)
                    if not isinstance(relation_object, Relationship):
                        values[field_name] = getattr(relation_object, primary_key_field_name)
                    else:
                        values[field_name] = None

            names[field_name] = self.__get_column_name(field_object, field_name)

        return types, names, values

    def _find_primary_key_of_table(self, table_name):
        fields = self.__all_data[table_name].items()

        for field_name, field_object in fields:
            if isinstance(field_object, PrimaryKey):
                column_name = self.__get_column_name(field_object, field_name)
                primary_key = [field_name, column_name, field_object.type]
                return primary_key

        # if we don't find the primary key field, the primary key is the first column
        for field_name, field_object in fields:
            if isinstance(field_object, Column):
                column_name = self.__get_column_name(field_object, field_name)
                primary_key = [field_name, column_name, field_object.type]
                return primary_key

        return None

    def _find_type_of_primary_key_of_relation(self, table_name):
        # we need to find a primary key of the table to which relationship is
        # in order to find the type of the field
        entity = self.__class_names.get(table_name)
        table_name = self._get_table_name(entity)
        primary_key = self._find_primary_key_of_table(table_name)
        assert primary_key is not None
        primary_key_field_name, primary_key_name, primary_key_type = primary_key
        return primary_key_field_name, primary_key_name, primary_key_type

    def _execute_query(self, query: str, query_type: str = 'QUERY'):
        if self.__is_connected:
            self.__database_connection.commit(query, query_type)
        else:
            print("CREATE A CONNECTION TO DATABASE!")

    def __get_all_inheritances(self, classes):
        inheritance_dictionary = dict()
        for base_class in classes:
            val = []
            for subclass in classes:
                if issubclass(base_class, subclass) and base_class != subclass:
                    val.append(subclass)
            if val:
                inheritance_dictionary[base_class] = val
        return inheritance_dictionary

    def __split_inheritance_data(self, entity):
        if type(entity) in self.__class_inheritance:
            all_values = [i for i in dir(entity) if
                          not i.startswith('__') and not i.endswith('__') and i not in dir(Entity)]
            all_values = [i for i in all_values if not isinstance(getattr(entity, i), Field)]

            grouped_values = self.__group_inheritance_values(all_values, entity)
            return grouped_values
        return None

    def __group_inheritance_values(self, values, entity):
        grouped_values = dict()
        inherit_from = self.__class_inheritance.get(type(entity))

        for i in range(len(inherit_from)):
            val = {}
            actual_table = self.__class_table_dict.get(inherit_from[i])
            for j in values:
                if j in inherit_from[i].__dict__:
                    val[j] = getattr(entity, j)
                if j in self.__all_data[actual_table] and j not in val:
                    val[j] = getattr(entity, j)
            grouped_values[actual_table] = val
        child_table_name = self._get_table_name(entity)
        child_values = {}
        for k, v in self.__all_data[child_table_name].items():
            if k in values:
                child_values[k] = getattr(entity, k)
        grouped_values[child_table_name] = child_values

        return grouped_values
