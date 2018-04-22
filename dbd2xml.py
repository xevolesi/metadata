import os.path
import argparse
from utils.helpers import save_ram_to_xml_result
from utils.DBD_to_RAM_translation import DBDToRAMTranslator
from utils.RAM_to_XML_translation import RAMToXMLTranslator


class DataBaseNotFoundError(ValueError):
    """Error for catching if we can't find data base to translation."""


parser_dbd2xml = argparse.ArgumentParser()
parser_dbd2xml.add_argument("dbd", action="store", help="Directory for .db file")
# parser_dbd2xml.add_argument("schema", action="store", help="Schema name in .db file")
parser_dbd2xml.add_argument("xml", action="store", help="Directory to .xml file")
args = parser_dbd2xml.parse_args()

if os.path.exists(args.dbd):
    # try:
    xml_repr = RAMToXMLTranslator(DBDToRAMTranslator(args.dbd).fetch_schema()).ram_to_xml()
    save_ram_to_xml_result(xml_repr, args.xml)
    # except TypeError:
    #     print("There's no schema with such name {} in {} file!".format(args.schema, os.path.basename(args.dbd)))
else:
    raise DataBaseNotFoundError("Can't find data base with such name: {}".format(os.path.basename(args.dbd)))
