from fields.relationship import ManyToMany
from manager.manager_helper_functions import get_column_name, get_table_name, find_primary_key_of_table


class TableMapper:

    def __init__(self, class_names, data):
        self.__class_names = class_names
        self.__data = data

    def get_o_relation(self, entity_name):
        entity = self.__class_names.get(entity_name)
        table_name = get_table_name(entity)
        return find_primary_key_of_table(self.__data, table_name)

    def find_many_to_many_relation_value(self, table_name):
        for key, value in self.__data[table_name].items():
            if isinstance(value, ManyToMany):
                value.name = get_column_name(value, key)
                return value
