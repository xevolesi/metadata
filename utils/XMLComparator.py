import unittest


class XMLComparator(unittest.TestCase):
    """Compares two xml files."""
    def __init__(self, first_xml, second_xml):
        self.first = first_xml      # generated
        self.second = second_xml    # source

    def compare(self):
        with open(self.second, "r", encoding="utf-8") as source:
            counter = 0
            for res, ori in zip(self.first, source):
                if counter == 1:
                    continue
                counter += 1
                self.assertTrue(res == ori,
                                msg="\nresult:\t\"{}\"\nis not equal\norigin:\t\"{}\"\nin line: {}".format(res[:-1],
                                                                                                           ori[:-1],
                                                                                                           counter))
