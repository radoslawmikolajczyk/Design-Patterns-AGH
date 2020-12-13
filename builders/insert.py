from typing import Union, Dict

from builders.stringutils import quote_database_object_name_unsafe


class InsertBuilder:
    __table: Union[str, None]
    __fields: Dict[str, str]

    def __init__(self):
        self.__fields = dict()
        self.__table = None

    def into(self, table: str) -> 'InsertBuilder':
        self.__table = table
        return self

    def add(self, field: str, string: str) -> 'InsertBuilder':
        self.__fields[field] = string
        return self

    def is_empty(self) -> bool:
        return len(self.__fields) == 0

    def build(self) -> str:
        assert self.__table is not None
        assert len(self.__fields) != 0

        fields = ', '.join(map(quote_database_object_name_unsafe, self.__fields.keys()))
        values = ', '.join(self.__fields.values())
        return f"INSERT INTO {quote_database_object_name_unsafe(self.__table)} ({fields}) VALUES ({values})"