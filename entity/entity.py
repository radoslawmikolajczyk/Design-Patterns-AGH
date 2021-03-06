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

    _table_name = ""
    _primary_key = None

    def get_primary_key(self):
        return self._primary_key
