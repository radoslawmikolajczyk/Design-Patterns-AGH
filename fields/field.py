class Field:
    pass


class Column(Field):
    """
        A class to represent a column in a database table

        Attributes
        -----------------
        type : StoreType
        nullable : Bool
        unique : Bool
        name : String

        Methods
        -----------------
        __init__(self, type, nullable=True, unique=False, name="DEFAULT"):
            initializes object with the given attributes
    """

    def __init__(self, type, nullable=True, unique=False, name="DEFAULT"):
        self.type = type
        self.nullable = nullable
        self.unique = unique
        self.name = name
