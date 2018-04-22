"""`mssql2pg` - перенос данных из БД MSSQL Server в БД PgSQL той же структуры.

Перенос данных из БД MS SQL Server в PostgreSQL.
    Здесь Вам следует:
    4.1. Взять БД "Northwind" - она в формате MS SQL Server
        (см. например здесь: https://northwinddatabase.codeplex.com/).
    4.2. Далее Вам следует обработать метаданные этой БД (см. MSDN)
    4.3. и сформировать объектное представление этих метаданных.
    4.4. Потом его можно преобразовать в XML для анализа"""
import pyodbc
import argparse
from utils.helpers import save_ram_to_xml_result
from utils.MSSQL_to_RAM_translation import MSSQLToRAMTranslator
from utils.RAM_to_XML_translation import RAMToXMLTranslator


parser_mssql2xml = argparse.ArgumentParser()
parser_mssql2xml.add_argument("driver", action="store", help="Drive for SQL Server")
parser_mssql2xml.add_argument("server", action="store", help="SQL Server")
parser_mssql2xml.add_argument("username", action="store", help="Username")
parser_mssql2xml.add_argument("database", action="store", help="Database name")
parser_mssql2xml.add_argument("xml", action="store", help="Path for saved xml-file")
args = parser_mssql2xml.parse_args()

connect = pyodbc.connect("DRIVER={driver};SERVER={server};DATABASE={database};UID={username};\
Trusted_Connection=yes".format(
    driver=args.driver,
    server=args.server,
    username=args.username,
    database=args.database))

# driver="ODBC Driver 13 for SQL Server",
# server="DESKTOP-GM2ENR3\SQLEXPRESS",
# username="DESKTOP-GM2ENR3\\user",
# database="Northwind"))

schema = MSSQLToRAMTranslator(connect).mssql_to_ram()
xml = RAMToXMLTranslator(schema).ram_to_xml()
save_ram_to_xml_result(xml, args.xml)
# save_ram_to_xml_result(xml, ".\\dbo.xml")
connect.close()
