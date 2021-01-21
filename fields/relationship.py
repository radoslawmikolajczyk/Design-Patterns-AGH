from .field import Field


class Relationship(Field):
    """
        A class to represent a relationship between database tables

        Attributes
        -----------------
        other : OneToOne / OneToMany / ManyToMany object
        name : String
    """

    def __init__(self, other, name="DEFAULT"):
        self.other = other
        self.name = name


class OneToOne(Relationship):
    def __init__(self, other, name):
        super().__init__(other, name)


class ManyToOne(Relationship):
    def __init__(self, other, name):
        super().__init__(other, name)


class ManyToMany(Relationship):
    def __init__(self, other, name):
        super().__init__(other, name)
