from typing import Union, Tuple, Dict

from builders.stringutils import quote_database_object_name_unsafe


class UpdateBuilder:
    __where: Union[Tuple[str, str], None]
    __table: Union[str, None]
    __fields: Dict[str, str]

    def __init__(self):
        self.__fields = dict()
        self.__table = None
        self.__where = None

    def table(self, table: str) -> 'UpdateBuilder':
        self.__table = table
        return self

    def add(self, field: str, string: str) -> 'UpdateBuilder':
        self.__fields[field] = string
        return self

    def where(self, field: str, string: str) -> 'UpdateBuilder':
        self.__where = (field, string)
        return self

    def is_empty(self) -> bool:
        return len(self.__fields) == 0

    def build(self) -> str:
        assert self.__table is not None
        assert self.__where is not None
        assert len(self.__fields) != 0
        field, value = self.__where
        set_fields = ', '.join(f"{quote_database_object_name_unsafe(c)} = {v}" for c, v in self.__fields.items())

        return f"UPDATE {quote_database_object_name_unsafe(self.__table)} SET {set_fields} WHERE {quote_database_object_name_unsafe(field)} = {value}"
