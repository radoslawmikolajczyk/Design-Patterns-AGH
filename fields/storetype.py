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
        pass

    def definition(self):
        pass


class Text(StoreType):

    def __init__(self, max_length=255, char_set="UTF-8"):
        super().__init__()
        self.max_length = max_length
        self.char_set = char_set


class TimeStamp(StoreType):

    def __init__(self, with_zone=False):
        super().__init__()
        self.with_zone = with_zone


class Integer(StoreType):

    def __init__(self):
        super().__init__()


class Float(StoreType):

    def __init__(self):
        super().__init__()


class Boolean(StoreType):

    def __init__(self):
        super().__init__()
