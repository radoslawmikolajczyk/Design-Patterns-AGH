class Entity:
    """
        A class to represent the database table entity
        
        Attributes
        -----------------
        name : String

        Methods
        -----------------
        __init__(self, name):
            initializes object with the given attributes
    """

    def __init__(self, name):
        self._table_name = name
