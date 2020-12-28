from connection.database import DatabaseConnection
from connection.configuration import ConnectionConfiguration
from entity.entity import Entity


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class Manager(metaclass=SingletonMeta):

    def __init__(self):
        self.database_connection = DatabaseConnection()

    def connect(self, conf: ConnectionConfiguration):
        pass

    def insert(self, entity: Entity):
        pass

    def delete(self, entity: Entity):
        pass

    def update(self, entity: Entity):
        pass

    def findById(self, model, id):

