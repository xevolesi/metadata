import argparse
import xml.dom.minidom as md
from utils.XML_to_RAM_translation import XMLToRAMTranslator
from data_base_sources.DBD_const import SQL_DBD_Init
from utils.RAM_to_DBD_translation import RAMToDBDTranslator


parser_xml2dbd = argparse.ArgumentParser()
parser_xml2dbd.add_argument("xml", action="store", help="Directory to .xml file")
parser_xml2dbd.add_argument("dbd", action="store", help="Directory for .db file")
args = parser_xml2dbd.parse_args()

# Parsing source xml file
xml = md.parse(args.xml)

# Getting RAMToDBDTranslator class's object to translate RAM to DBD.
dbd_repr = RAMToDBDTranslator(XMLToRAMTranslator(xml).xml_to_ram(), args.dbd, SQL_DBD_Init).ram_to_dbd()
