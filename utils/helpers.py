"""Helps to understand how the data base is."""
import os.path
from collections import Counter


def save_ram_to_xml_result(file, filename):
    """
    :param      file: file to save.

    :param      filename: name of xml file.

    :return:    has not.

    Saves the result of RAM → XML translation.
    """
    with open(filename, "wb") as f:
        f.write(file.toprettyxml(encoding="utf-8", indent="  "))


def get_source_xml_file_path(filename):
    return os.path.join("source_xmls", filename)


def get_list_of_domain_types(schema_domains):
    """
    :param      schema_domains: DBClasses.Schema().domains list.

    :return:    unique_types: list of domains types.
    """
    domain_type_list = list()
    for domain in schema_domains:
        domain_type_list.append(domain.type)

    unique_types = list()
    for key in Counter(domain_type_list).keys():
        unique_types.append(key)

    return unique_types
