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
        self.__all_data, self.__class_names = self.__get_all_instances(Entity)
        self.__junction_tables = []
        self.__junction_tables_names = []
        # self.__class_inheritance -- dict for all classes (without Entity) which inherit,
        # ex. { <class '__main__.Address'> : [<class '__main__.City'>, <class '__main__.Street'>]}
        self.__class_inheritance = {}

    # create table for a given class
    def create_table(self, entity: Entity):
        table_name = self._get_table_name(entity)
        values_from_all_data = self.__all_data[table_name]
        data = {table_name: values_from_all_data}
        self._create_tables_mapper(data)

    def _create_tables_mapper(self, data):
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
            print(self.__all_data)
            print(self.__class_names)
            print(self.__junction_tables)
            print(self.__junction_tables_names)
        else:
            print("CREATE A CONNECTION TO DATABASE!")

    def create_tables(self):
        self._create_tables_mapper(self.__all_data)

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
                    second_fk_values = getattr(entity, field_name)
                    second_fk_name = self.__get_column_name(field_object, field_name)

                    junction_table_name = self._find_name_of_junction_table(table_name, second_table_name)

                    if not isinstance(second_fk_values, ManyToMany):
                        for second_value in second_fk_values:
                            builder = InsertBuilder().into(junction_table_name)
                            builder.add(first_fk_name, first_fk_type, first_fk_value)
                            builder.add(second_fk_name, second_fk_type, second_value)
                            self._execute_query(builder.build(), 'INSERT')

                    break

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

        primary_key = self._find_primary_key_of_table(table_name)
        assert primary_key is not None
        primary_key_field_name, primary_key_name, primary_key_type = primary_key
        primary_key_value = getattr(entity, primary_key_field_name)
        primary_key_saved_value = entity.get_primary_key()
        entity._primary_key = primary_key_value

        builder = None

        if query_type is 'INSERT':
            builder = InsertBuilder().into(table_name)
        elif query_type is 'UPDATE':
            builder = UpdateBuilder().table(table_name)
        elif query_type is 'DELETE':
            builder = DeleteBuilder().table(table_name)

        assert builder is not None

        if query_type in ('INSERT', 'UPDATE'):
            types, names = self._find_names_and_types_of_columns(table_name)
            for field_name in types.keys():
                store_type = types[field_name]
                column_name = names[field_name]
                value = getattr(entity, field_name)

                # case for values that are not already assigned (NULL)
                if not isinstance(value, Column) and \
                        not isinstance(value, PrimaryKey) and \
                        not isinstance(value, Relationship):
                    builder.add(column_name, store_type, value)

        if query_type in ('UPDATE', 'DELETE'):
            builder.where(primary_key_name, primary_key_type, primary_key_saved_value)

        self._execute_query(builder.build(), query_type)

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
        types, names = self._find_names_and_types_of_columns(table_name)
        builder = self.__build_regular(table_name, types, names, key_name, key_value)

        if self.__check_got_many_relation(table_name):
            self.__build_many_relation(table_name, key_name, builder)

        query, fields = builder.build()
        return self.select(model, query)

    def __check_got_many_relation(self, table_name):
        for junction_name in self.__junction_tables_names:
            if table_name in junction_name:
                return True
        return False

    def __build_regular(self, table_name, types, names, key_name, key_value):
        builder = SelectBuilder().table(table_name)
        for field_name in types.keys():
            builder.add(table_name, names[field_name])
        builder.where(names[key_name], types[key_name], key_value)
        return builder

    def __build_many_relation(self, table_name, key_name, builder):
        junction_data = self._get_junction_data(table_name)
        other = self.__find_many_to_many_other_field(table_name)

        for index, data in enumerate(junction_data):
            junction_table_name, junction_key= data
            builder.join(junction_table_name, junction_key, table_name, key_name)

            builder.add(junction_table_name, other[index].name)

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

    def __find_many_to_many_other_field(self, table_name):
        names = []
        for key, value in self.__all_data[table_name].items():
            if isinstance(value, ManyToMany):
                names.append(value)
        return names

    def select_raw(self, query: str) -> list:
        if self.__is_connected:
            query_result = self.__database_connection.execute(query)
            return query_result
        else:
            print("CREATE A CONNECTION TO DATABASE!")
            return []

    def select(self, model: Entity, query: str) -> list:
        if self.__is_connected:
            table_name = self._get_table_name(model)
            _, names = self._find_names_and_types_of_columns(table_name)
            query_result = self.select_raw(query)
            result = self.__map_result_fields(model, names, query_result)
            return result
        else:
            print("CREATE A CONNECTION TO DATABASE!")
            return []

    def __map_result_fields(self, model: Entity, fields_columns_map, query_result: QueryResult):
        table_name = self._get_table_name(model)
        _, pk_column_name, _ = self._find_primary_key_of_table(table_name)
        fetched = query_result.get_query()

        result = self.__prepare_select_result(model, pk_column_name, fetched)
        for row in fetched:
            primary_key = row[pk_column_name]
            obj = result[primary_key]
            for key, value in fields_columns_map.items():
                field_type = getattr(model, key)
                # print("?????????????????????????", row, field_type, model, key, value, (value not in row))
                if value not in row:
                    continue
                if isinstance(field_type, OneToOne) or isinstance(field_type, ManyToOne):
                    other = self.find_by_id(self.__class_names[field_type.other], row[value])
                    setattr(obj, key, other)
                elif isinstance(field_type, ManyToMany):
                    obj_field = getattr(obj, key)
                    if isinstance(obj_field, list):
                        setattr(obj, key, obj_field.append(row[value]))
                    elif self.__is_column(obj_field):
                        setattr(obj, key, row[value])
                    else:
                        setattr(obj, key, [obj_field, row[value]])
                else:
                    setattr(obj, key, row[value])

        return list(result.values())

    def __prepare_select_result(self, model: Entity, pk_column_name, fetched_values):
        result = {}
        for row in fetched_values:
            obj = deepcopy(model)
            primary_key = row[pk_column_name]
            obj._primary_key = primary_key
            result[primary_key] = obj
        return result


        # table_name = self._get_table_name(model)
        # pk_field_name, pk_column_name, pk_type = self._find_primary_key_of_table(table_name)
        # aggregate = {}
        # records = query_result.get_query()
        # mapped = [deepcopy(model) for _ in records]

        # pk_index = list(field_names).index(pk_field_name)

        # for i, record in enumerate(records):
        #     for j, field in enumerate(record):
        #         setattr(mapped[i], list(field_names)[j], field)
        #         record_pk = record[pk_index]
        #         if record_pk in aggregate:
        #             in_dict = getattr(aggregate[record_pk], list(field_names)[j])
        #             if self._is_column(in_dict):
        #                 # this also changes the object in mapped list, because of the way we
        #                 # added this item to the dict
        #                 setattr(aggregate[record_pk], list(field_names)[j], field)
        #             elif in_dict != field and not isinstance(in_dict, list):
        #                 setattr(aggregate[record_pk], list(field_names)[j], [in_dict, field])
        #             elif in_dict != field and isinstance(in_dict, list) and field not in in_dict:
        #                 new = in_dict
        #                 new.append(field)
        #                 setattr(aggregate[record_pk], list(field_names)[j], new)
        #         else:
        #             # if we add the mapped object to dict this way we essentialy have a dict like
        #             # {primary_key_value: pointer_to_object_in_mapped_list}
        #             mapped[i]._primary_key = record_pk
        #             aggregate[record_pk] = mapped[i]

        # return list(aggregate.values())

    def __is_column(self, field_value):
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
            elif isinstance(field_object, ManyToOne) or isinstance(field_object, OneToOne):
                primary_key_type = self._find_type_of_primary_key_of_relation(field_object.other)
                types[field_name] = primary_key_type

            names[field_name] = self.__get_column_name(field_object, field_name)

        return types, names

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
        _, _, primary_key_type = primary_key
        return primary_key_type

    def _execute_query(self, query: str, query_type: str = 'QUERY'):
        if self.__is_connected:
            self.__database_connection.commit(query, query_type)
        else:
            print("CREATE A CONNECTION TO DATABASE!")

    def _has_inheritance(self, cls):
        inheritance_list = []
        for key, value in self.__class_names.items():
            if isinstance(cls, value) and value != type(cls):
                inheritance_list.append(value)
        if len(inheritance_list) > 0:
            self.__class_inheritance[type(cls)] = inheritance_list
            return True
        return False

    '''
        Do funkcji przekazujemy obiekt i robimy skan po wszystkich co dziedziczy (jesli dziedziczy ofc), 
        funkcja zwraca nam slownik (jesli dane entity dziedziczy po czyms, inaczej zwraca None) w formacie:
        { nazwa_tabeli_rodzic1 : { nazwa_kolumny: wartosc_wprowadzona_przez_uzytk }, nazwa_tabeli_rodzic2 : {...}, nazwa_tabeli_dziecko: {} }
        Mozna ten slownik juz bezposrednio obsluzyc w operacjach ddl,
    '''
    def _get_inheritance_data(self, cls):
        if self._has_inheritance(cls):
            # pobieramy wszystkie wartosci z obiektu (w tym tez te z klas parent)
            all_values = [i for i in dir(cls) if
                          not i.startswith('__') and not i.endswith('__') and i not in dir(Entity)]
            # sprawdzamy czy wartosci sa typu Field, jesli sa to skip. Bierzemy tylko to co jest uzywane
            all_values = [i for i in all_values if not isinstance(getattr(cls, i), Field)]

            # wartosci to slownik, w formacie { nazwa_tabeli: {nazwa_pola : wartosc(JUZ NIE COLUMN, tylko np
            # string), itd..}}
            grouped_values = self._find_parent_values(all_values, cls)
            own_values = {}

            print(all_values)
            table_name = self._get_table_name(cls)
            for k, v in self.__all_data[table_name].items():
                if k in all_values:
                    own_values[self.__get_column_name(v, k)] = getattr(cls, k)

            grouped_values[table_name] = own_values

            return grouped_values
        return None

    def _find_parent_values(self, array, cls):
        parent_classes = self.__class_inheritance.get(type(cls))
        parent_objects = []
        for i in range(len(parent_classes)):
            parent_objects.append(parent_classes[i]())
        table_names = []
        for i in range(len(parent_objects)):
            table_names.append(self._get_table_name(parent_objects[i]))

        values_from_all_data = {}
        parent_dictionary = {}
        for i in range(len(table_names)):
            new_dict = {}
            values_from_all_data[table_names[i]] = self.__all_data[table_names[i]]
            parent_dictionary[table_names[i]] = new_dict

            for k, v in values_from_all_data[table_names[i]].items():
                if k in array:
                    var = getattr(cls, array[array.index(k)])
                    if var:
                        new_dict[self.__get_column_name(v, k)] = var

        del parent_objects
        return parent_dictionary
