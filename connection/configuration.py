class ConnectionConfiguration:
    def __init__(self, user, password, database, host='127.0.0.1', port='5432'):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port
