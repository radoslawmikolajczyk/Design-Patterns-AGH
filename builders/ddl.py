from typing import Union, Tuple, List

from builders.stringutils import quote_database_object_name_unsafe, generate_escape_seq
from fields.storetype import StoreType


class DDLBuilder:
    __name: Union[str, None]
    __fields: List[Tuple[str, StoreType]]
    __primary_key: Union[str, None]
    __foreign_keys: List[Tuple[str, Tuple[str, str]]]

    def __init__(self):
        self.__name = None
        self.__fields = []
        self.__foreign_keys = []
        self.__primary_key = None

    def name(self, name: str) -> 'DDLBuilder':
        self.__name = name
        return self

    def field(self, name: str, stype: StoreType) -> 'DDLBuilder':
        self.__fields.append((name, stype))
        return self

    def primary_key(self, name: str) -> 'DDLBuilder':
        self.__primary_key = name
        return self

    def foreign_keys(self, name: str, table: str, other_name: str) -> 'DDLBuilder':
        self.__foreign_keys.append((name, (other_name, table)))
        return self

    def is_empty(self) -> bool:
        return len(self.__fields) == 0

    def build(self) -> str:
        assert self.__name is not None
        assert len(self.__fields) > 0
        assert self.__primary_key is not None
        fields = ', '.join(
            [f"{quote_database_object_name_unsafe(nam)} {typ.definition()}" for (nam, typ) in self.__fields])
        f_keys = ', '.join([
            f"CONSTRAINT fk_{generate_escape_seq()} FOREIGN KEY({quote_database_object_name_unsafe(loc)}) REFERENCES {quote_database_object_name_unsafe(other)}({quote_database_object_name_unsafe(nam)})"
            for loc, (nam, other) in self.__foreign_keys])
        print(len(f_keys))
        return f"CREATE TABLE {quote_database_object_name_unsafe(self.__name)} ({fields}, PRIMARY KEY({quote_database_object_name_unsafe(self.__primary_key)}){', ' if len(f_keys) > 0 else ''}{f_keys})"
