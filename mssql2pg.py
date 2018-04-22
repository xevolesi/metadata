"""
4.5. и сформировать DDL  PostgreSQL, для генерации в PostgreSQL БД идентичной Northwind структуры.
4.6. Следующим шагом нужно перенести данные из БД MS SQL Server в БД PostgreSQL, созданной в п.4.5."""
import pyodbc
import psycopg2
import datetime
import argparse
from utils.MSSQL_to_RAM_translation import MSSQLToRAMTranslator
from utils.RAM_to_PostgreSQL_translation import RAMToPostgreSQLTranslator
from utils.RAM_to_XML_translation import RAMToXMLTranslator


parser_mssql2pg = argparse.ArgumentParser()
parser_mssql2pg.add_argument("driver", action="store", help="Drive for SQL Server")
parser_mssql2pg.add_argument("server", action="store", help="SQL Server")
parser_mssql2pg.add_argument("username", action="store", help="Username")
parser_mssql2pg.add_argument("database", action="store", help="Database name")
parser_mssql2pg.add_argument("dbname", action="store", help="Database name")
parser_mssql2pg.add_argument("user", action="store", help="Database name")
parser_mssql2pg.add_argument("host", action="store", help="Database name")
parser_mssql2pg.add_argument("pwd", action="store", help="Database name")
args = parser_mssql2pg.parse_args()
# Достаем из Northwind объектное представление метаданных.
connectMSSQL = pyodbc.connect("DRIVER={driver};SERVER={server};DATABASE={database};UID={username};\
Trusted_Connection=yes".format(
    # driver="ODBC Driver 13 for SQL Server",
    # server="DESKTOP-GM2ENR3\SQLEXPRESS",
    # username="DESKTOP-GM2ENR3\\user",
    # database="Northwind"
    driver=args.driver,
    server=args.server,
    username=args.username,
    database=args.database
))
schema = MSSQLToRAMTranslator(connectMSSQL).mssql_to_ram()
xml = RAMToXMLTranslator(schema).ram_to_xml()


# # Генерируем пустую базу данных в PostgreSQL со структурой, идентичной структуре Northwind в MSSQL.
connectPostgreSQL = psycopg2.connect("dbname='{dbname}' user='{user}' host='{host}' password='{password}'".format(
               # dbname="postgres",
               # user="postgres",
               # host="localhost",
               # password="123"
               dbname=args.dbname,
               user=args.user,
               host=args.host,
               password=args.pwd
           ))
t = RAMToPostgreSQLTranslator(schema, "dbo")
cursorPostgreSQL = connectPostgreSQL.cursor()
cursorPostgreSQL.execute(t.ram_to_postgresql())
print(t.ram_to_postgresql())
#
# connectPostgreSQL.commit()
# connectPostgreSQL.close()
#
# # TODO: Выкачка данных из MSSQL в PosgreSQL.
cursorMSSQL = connectMSSQL.cursor()
# for i in cursorMSSQL.execute("select * from dbo.[Order Details]").fetchall():
#     print(i)

# cursorPostgreSQL.execute("""insert into dbo.Employees (EmployeeID, LastName)
# values (%s, %s)""", (1, "Strakhov"))
cursorPostgreSQL.execute("begin transaction;")
for table in schema.tables:
    query_fields = ", ".join([field.name if field.domain.type != "BLOB" else "MASTER.dbo.Fn_varbintohexstr("
                                                                             + field.name + ")" for field in table.fields])
    select_query = " ".join(["""select {fields} from""", "".join(["\"" + table.name + "\"",
                                                                  ";"])]).format(fields=query_fields)
    query_fields = ", ".join([field.name for field in table.fields])
    # print(query_fields)
    # print(select_query)
    t = cursorMSSQL.execute(select_query).fetchall()
    if t:
        for record in t:
            # print(record)
            converted_record = list()
            for item in record:
                if type(item) == datetime.datetime:
                    converted_record.append(str(item)[:-9])
                # elif type(item) == bytes:
                #     converted_record.append("decode(" + str(item).replace("b", "")+",'hex')")
                #     print("".join(["decode(", str(item).replace("b", ""), ",", "'hex')"]))
                else:
                    converted_record.append(item)
            disable_triggers = """alter table {table} 
            disable trigger all;""".format(table=".".join([schema.name,
                                                           "".join([symbol for symbol in table.name
                                                                    if symbol not in "\/.- "])]))
            insert_query = """
            insert into {table} ({fields}) values {values};""".format(
                table=".".join([schema.name, "".join([symbol for symbol in table.name if symbol not in "\/.- "])]),
                fields=query_fields,
                values="".join(["(", ", ".join(["%s" for i in range(len(converted_record))]), ")"])
                )
            enable_triggers = """alter table {table} 
            enable trigger all;""".format(table=".".join([schema.name,
                                                          "".join([symbol for symbol in table.name
                                                                   if symbol not in "\/.- "])]))
            # print(insert_query)
            cursorPostgreSQL.execute(disable_triggers)
            cursorPostgreSQL.execute(insert_query, tuple(converted_record))
            cursorPostgreSQL.execute(enable_triggers)
cursorPostgreSQL.execute("commit;")
