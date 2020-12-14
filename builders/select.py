from typing import Union, Tuple, List

from builders.stringutils import quote_database_object_name_unsafe
from fields.storetype import StoreType


class SelectBuilder:
    __where: Union[Tuple[str, str], None]
    __from: Union[str, None]
    __joins: List[Tuple[Tuple[str, str], Tuple[str, str]]]
    __fields: List[Tuple[str, str]]

    def __init__(self):
        self.__from = None
        self.__where = None
        self.__joins = []
        self.__fields = []

    def table(self, table: str) -> 'SelectBuilder':
        self.__from = table
        return self

    def add(self, table: str, field: str) -> 'SelectBuilder':
        self.__fields.append((table, field))
        return self

    def where(self, field: str, typ: StoreType, data) -> 'SelectBuilder':
        self.__where = (field, typ.serialize(data))
        return self

    def join(self, table: str, field: str, on_table: str, on_field: str) -> 'SelectBuilder':
        self.__joins.append(((table, field), (on_table, on_field)))
        return self

    def is_empty(self) -> bool:
        return len(self.__fields) == 0

    def build(self) -> Tuple[str, List[Tuple[str, str]]]:
        assert self.__from is not None
        assert self.__where is not None
        assert len(self.__fields) != 0
        field, value = self.__where
        joins = ' '.join(
            f"JOIN {quote_database_object_name_unsafe(tabA)} ON ({quote_database_object_name_unsafe(tabA)}.{quote_database_object_name_unsafe(fldA)} = {quote_database_object_name_unsafe(tabB)}.{quote_database_object_name_unsafe(fldB)})"
            for (tabA, fldA), (tabB, fldB) in self.__joins)
        fields = ', '.join(
            f"{quote_database_object_name_unsafe(tab)}.{quote_database_object_name_unsafe(fld)}" for tab, fld in
            self.__fields)
        query = f"SELECT {fields} FROM {quote_database_object_name_unsafe(self.__from)} {joins} WHERE {quote_database_object_name_unsafe(self.__from)}.{quote_database_object_name_unsafe(field)} = {value}"
        return query, self.__fields.copy()
