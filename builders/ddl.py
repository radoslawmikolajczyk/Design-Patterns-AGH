from typing import Union, Tuple, List

from builders.stringutils import quote_database_object_name_unsafe, generate_escape_seq
from fields.storetype import StoreType


class DDLBuildable:

    def build(self):
        raise NotImplementedError()


class DDLForeignKeyBuildable(DDLBuildable):
    __name: str
    __other_table: str
    __other_name: str

    def __init__(self, name: str, other_table: str, other_name: str):
        self.__name = quote_database_object_name_unsafe(name)
        self.__other_table = quote_database_object_name_unsafe(other_table)
        self.__other_name = quote_database_object_name_unsafe(other_name)

    def build(self):
        return f"CONSTRAINT fk_{generate_escape_seq()} FOREIGN KEY({self.__name}) REFERENCES {self.__other_table}({self.__other_name})"


class DDLUniqueBuildable(DDLBuildable):
    __name: str

    def __init__(self, name: str):
        self.__name = quote_database_object_name_unsafe(name)

    def build(self):
        return f"CONSTRAINT uk_{generate_escape_seq()} UNIQUE ({self.__name})"


class DDLPrimaryKeyBuildable(DDLBuildable):
    __name: str

    def __init__(self, name: str):
        self.__name = quote_database_object_name_unsafe(name)

    def build(self):
        return f"PRIMARY KEY({self.__name})"


class DDLBuilder:
    __name: Union[str, None]
    __fields: List[Tuple[str, StoreType]]
    __constraints: List[DDLBuildable]
    __primary_key: Union[DDLPrimaryKeyBuildable, None]

    def __init__(self):
        self.__name = None
        self.__fields = []
        self.__constraints = []
        self.__primary_key = None

    def name(self, name: str) -> 'DDLBuilder':
        self.__name = name
        return self

    def field(self, name: str, stype: StoreType, nullable: bool = True) -> 'DDLBuilder':
        self.__fields.append(DDLField(name, stype, nullable))
        return self

    def primary_key(self, name: str) -> 'DDLBuilder':
        self.__primary_key = DDLPrimaryKeyBuildable(name)
        return self

    def foreign_key(self, name: str, other_table: str, other_name: str) -> 'DDLBuilder':
        self.__constraints.append(DDLForeignKeyBuildable(name, other_table, other_name))
        return self

    def unique(self, name: str) -> 'DDLBuilder':
        self.__constraints.append(DDLUniqueBuildable(name))
        return self

    def is_empty(self) -> bool:
        return len(self.__fields) == 0

    def build(self) -> str:
        assert self.__name is not None
        assert len(self.__fields) > 0
        assert self.__primary_key is not None
        fields = ', '.join(
            [f"{quote_database_object_name_unsafe(nam)} {typ.definition()}" for (nam, typ) in self.__fields])
        f_keys = ', '.join([c.build() for c in [self.__primary_key, *self.__constraints]])

        return f"CREATE TABLE {quote_database_object_name_unsafe(self.__name)} ({fields}, {f_keys})"
