class QueryResult:
    def __init__(self, query):
        self.__query = query

    def change_query(self, query):
        self.__query = query

    def get_query(self):
        return self.__query
