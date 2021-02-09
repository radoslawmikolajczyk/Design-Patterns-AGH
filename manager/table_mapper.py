from copy import deepcopy

from entity.entity import Entity
from fields.relationship import OneToOne, ManyToOne, ManyToMany
from connection.query import QueryResult
from manager.manager_helper_functions import get_column_name, get_table_name, find_primary_key_of_table, \
    get_object_pk, get_field_names_dict


class TableMapper:
    def __init__(self, class_names, data, manager):
        self.__class_names = class_names
        self.__data = data
        self.manager = manager
        self.inheritance = {}

    def get_o_relation(self, entity_name):
        entity = self.__class_names.get(entity_name)
        table_name = get_table_name(entity)
        return find_primary_key_of_table(self.__data, table_name)

    def find_many_to_many_relation_value(self, table_name):
        for key, value in self.__data[table_name].items():
            if isinstance(value, ManyToMany):
                value.name = get_column_name(value, key)
                return value
    
    def map_result_fields(self, model: Entity, fields_columns_map, query_result: QueryResult, many_keys=False,
                            junction_data=None):
        table_name = get_table_name(model)
        _, pk_column_name, _ = find_primary_key_of_table(self.__data, table_name)
        fetched = query_result.get_query()
        result = self.__prepare_select_result(model, pk_column_name, fetched)
        for row in fetched:
            primary_key = row[pk_column_name]
            obj = result[primary_key]
            for key, value in fields_columns_map.items():
                field_type = getattr(model, key)
                if isinstance(field_type, OneToOne) or isinstance(field_type, ManyToOne):
                    other = self.manager.find_by_id(self.__class_names[field_type.other], row[value])
                    setattr(obj, key, other)
                elif isinstance(field_type, ManyToMany):
                    self.__map_many_keys(obj, key, row[value])
                else:
                    setattr(obj, key, row[value])
            result[primary_key] = obj

        if not many_keys:
            for obj in list(result.values()):
                self.__map_many_objects(obj, junction_data, model, fields_columns_map)

        return list(result.values())

    def __prepare_select_result(self, model: Entity, pk_column_name, fetched_values):
        result = {}
        for row in fetched_values:
            obj = deepcopy(model)
            primary_key = row[pk_column_name]
            obj._primary_key = primary_key
            result[primary_key] = obj
        return result

    def __map_many_keys(self, obj, key, value):
        obj_field = getattr(obj, key)
        if isinstance(obj_field, list):
            new = obj_field.copy()
            new.append(value)
            setattr(obj, key, new)
        else:
            setattr(obj, key, [value])

    def __map_many_objects(self, obj, junction_data, model, fields_columns_map):
        for key, value in fields_columns_map.items():
            field_type = getattr(model, key)
            if not isinstance(field_type, ManyToMany):
                continue

            this_mapped, other_mapped, other_junction_key = self.__map_related_objects(obj, model, key, junction_data)
            self.__link_relation_objects(this_mapped, other_mapped, key, other_junction_key)

    def __map_related_objects(self, obj, model, key, junction_data):
        table_name = get_table_name(model)
        primary_key = get_object_pk(obj, model, self.__data)

        field_type = getattr(model, key)
        this_stack = []
        other_stack = getattr(obj, key).copy()
        this_mapped = {primary_key: obj}
        other_mapped = {}
        mapping = "other"
        other_class = self.__class_names[field_type.other]()
        other_junction_col = self.__extract_other_junction_key(junction_data, table_name, get_table_name(
            self.__class_names[field_type.other]))
        other_junction_key = self.__get_field_name(other_class, other_junction_col)

        while len(this_stack + other_stack) != 0:
            if mapping == "other" or len(this_stack) == 0:
                rel_key = other_stack.pop()
                rel_obj = self.manager.find_by_id(other_class, rel_key, True)
                other_mapped[rel_key] = rel_obj
                for i in getattr(rel_obj, other_junction_key):
                    if i not in this_mapped.keys():
                        this_stack.append(i)
                mapping = "this"

            elif mapping == "this" or len(other_stack) == 0:
                rel_key = this_stack.pop()
                rel_obj = self.manager.find_by_id(model, rel_key, True)
                this_mapped[rel_key] = rel_obj
                for i in getattr(rel_obj, key):
                    if i not in other_mapped.keys():
                        other_stack.append(i)
                mapping = "other"

        return this_mapped, other_mapped, other_junction_key

    def __extract_other_junction_key(self, junction_data, table_name, other_name):
        for dataset in junction_data:
            junction_name = dataset[0]
            if table_name in junction_name and other_name in junction_name:
                return dataset[1]

    def __get_field_name(self, model, search_column):
        names = get_field_names_dict(model, self.__data, self.__class_names, self.inheritance)
        for field_name, column_name in names.items():
            if column_name == search_column:
                return field_name

    def __link_relation_objects(self, left, right, left_key, right_key):
        for mapped in left.values():
            new_val = []
            for i in getattr(mapped, left_key):
                new_val.append(right[i])
            setattr(mapped, left_key, list(set(new_val)))

        for mapped in right.values():
            new_val = []
            for i in getattr(mapped, right_key):
                new_val.append(left[i])
            setattr(mapped, right_key, list(set(new_val)))
