import builders.stringutils as strutl
import datetime

class StoreType:
    """
        A class to represent a type of column in a database table

        Attributes
        -----------------
        _python_type : type
        _database_type : string
        nullable : Bool

        Methods
        -----------------
        parse -> ?
        definition -> ?

    """

    def __init__(self):
        self._python_type = ""
        self._database_type = ""
        self.nullable = False

    def parse(self):
        raise NotImplemented()

    def serialize(self, data):
        raise NotImplemented()

    def definition(self):
        raise NotImplemented()


class Text(StoreType):

    def __init__(self, max_length=255, char_set="UTF-8"):
        super().__init__()
        self.max_length = max_length
        self.char_set = char_set

    def serialize(self, data):
        assert type(data) is str
        assert len(data) <= self.max_length
        return strutl.quote_string(data)



class TimeStamp(StoreType):

    def __init__(self, with_zone=False):
        super().__init__()
        self.with_zone = with_zone

    def serialize(self, data):
        assert type(data) is datetime.datetime

        if self.with_zone:
            return f"TIMESTAMP WITH TIME ZONE '{data.strftime('%Y-%m-%d %H:%M:%S%z')}'"
        else:
            return f"TIMESTAMP '{data.astimezone(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}'"


class Integer(StoreType):

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        assert type(data) is int
        return str(data)


class Float(StoreType):

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        assert type(data) is float
        return str(data)


class Boolean(StoreType):

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        assert type(data) is bool
        if data:
            return "TRUE"
        else:
            return "FALSE"
