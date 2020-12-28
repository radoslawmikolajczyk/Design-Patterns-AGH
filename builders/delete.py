from typing import Union, Tuple, List

from builders.stringutils import quote_database_object_name_unsafe
from fields.storetype import StoreType


class DeleteBuilder:
    __from: Union[str, None]
    __where: Union[Tuple[str,str], None]

    def __init__(self):
        self.__from = None
        self.__where = None

    def table(self, table: str) -> 'DeleteBuilder':
        self.__from = table
        return self

    def where(self, field: str, typ: StoreType, data) -> 'DeleteBuilder':
        self.__where = (field, typ.serialize(data))
        return self

    def build(self) -> str:
        assert self.__from is not None
        assert self.__where is not None
        field, value = self.__where
        return f"DELETE FROM {quote_database_object_name_unsafe(self.__from)} WHERE {quote_database_object_name_unsafe(field)} = {value}"