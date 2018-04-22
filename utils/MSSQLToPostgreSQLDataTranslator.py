class MSSQLToPostgreSQLDataTranslator:
    def __init__(self, mssql_ram_rep, connection):
        self.ram_repr = mssql_ram_rep
        self.connection = connection


