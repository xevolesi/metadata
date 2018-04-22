import os
import argparse
import psycopg2
from utils.RAM_to_PostgreSQL_translation import RAMToPostgreSQLTranslator
from utils.DBD_to_RAM_translation import DBDToRAMTranslator


parser_dbd2db = argparse.ArgumentParser()
parser_dbd2db.add_argument("dbname", action="store", help="Data base name")
parser_dbd2db.add_argument("user", action="store", help="User name")
parser_dbd2db.add_argument("host", action="store", help="Host name")
parser_dbd2db.add_argument("pwd", action="store", help="Password")
parser_dbd2db.add_argument("dbd", action="store", help="Directory for .db file")

args = parser_dbd2db.parse_args()

if os.path.exists(args.dbd):
    schema = DBDToRAMTranslator(args.dbd).fetch_schema()

    connect = psycopg2.connect("dbname='{dbname}' user='{user}' host='{host}' password='{password}'".format(
        dbname=args.dbname,
        user=args.user,
        host=args.host,
        password=args.pwd
        # dbname="postgres",
        # user="postgres",
        # host="localhost",
        # password="123"
           ))

    t = RAMToPostgreSQLTranslator(schema, os.path.basename(args.dbd).split(".")[0].lower())
    cursor = connect.cursor()
    cursor.execute(t.ram_to_postgresql())
    print(t.ram_to_postgresql())
else:
    raise FileNotFoundError("Can't find data base with {} name!".format(os.path.basename(args.dbd)))
