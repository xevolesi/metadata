import data_base_sources.DBClasses as Dbc


class DBException(Exception):
    """Base class for exceptions which may be thrown while parsing .xml file."""
    def __init__(self, expected, got):
        self.expected = expected
        self.got = got


class WrongAttributeException(DBException):
    """Describes exception which may be thrown if we find wrong attribute while parsing."""

    def __str__(self):
        return "Expected one of {} attributes, but found {} attribute!".format(self.expected, self.got)


class WrongNodeException(DBException):
    """Describes exception which may be thrown if we find wrong node name while parsing."""

    def __str__(self):
        return "Expected {} node, but found {} node!".format(self.expected, self.got)


class WrongPropertyException(DBException):
    """Describes exception which may be thrown if we find wrong property while parsing."""

    def __str__(self):
        return "Expected one of {} properties, but found {} property!".format(self.expected, self.got)


class XMLToRAMTranslator:
    """Translates XML → RAM."""

    def __init__(self, xml_file):
        self.xml_repr = xml_file
        self.domain_list = self.get_domains()
        self.tables_list = self.get_tables()

    def xml_to_ram(self):
        return self.get_schema()

    def get_domains(self):
        """
        :return:    domain_list: list of domains.

        Parse an xml file to find domains in.
        """
        domain_list = list()
        for domain in self.xml_repr.getElementsByTagName("domain"):
            dom = Dbc.Domain()
            for an, av in domain.attributes.items():
                if an.lower() == "name":
                    dom.name = av
                elif an.lower() == "description":
                    dom.description = av
                elif an.lower() == "type":
                    dom.type = av
                elif an.lower() == "align":
                    dom.align = av
                elif an.lower() == "width":
                    dom.width = av
                elif an.lower() == "props":
                    for prop in av.split(", "):
                        if prop == "show_null":
                            dom.show_null = True
                        elif prop == "summable":
                            dom.summable = True
                        elif prop == "case_sensitive":
                            dom.case_sensitive = True
                        elif prop == "show_lead_nulls":
                            dom.show_lead_nulls = True
                        elif prop == "thousand_separator":
                            dom.thousands_separator = True
                        else:
                            raise WrongPropertyException(
                                ["show_null", "summable", "case_sensitive", "show_lead_nulls", "thousands_separator"],
                                prop)
                elif an.lower() == "char_length":
                    dom.char_length = av
                elif an.lower() == "length":
                    dom.length = av
                elif an.lower() == "precision":
                    dom.precision = av
                elif an.lower() == "scale":
                    dom.scale = av
                else:
                    raise WrongAttributeException(
                        ["props", "scale", "length", "char_length", "precision", "width", "align",
                         "type", "description", "name"], an)
            domain_list.append(dom)

        return domain_list

    @staticmethod
    def get_indices(table):
        """
        :param      table: Dbc.Table() to find indices in.

        :return:    indices_list: list of indices in table.

        Parse table to find indices in.
        """
        if table.nodeName != "table":
            raise WrongNodeException("table", table.nodeName)

        indices_list = list()
        for index in table.getElementsByTagName("index"):
            idx = Dbc.Index()
            for an, av in index.attributes.items():
                if an.lower() == "name":
                    idx.name = av
                elif an.lower() == "field":
                    idx.field = av
                elif an.lower() == "props":
                    for prop in av.split(", "):
                        if prop == "fulltext":
                            idx.fulltext = True
                        elif prop == "uniqueness":
                            idx.uniqueness = True
                        elif prop == "local":
                            idx.local = True
                        else:
                            raise WrongPropertyException(["fulltext", "uniqueness", "local"], prop)
                else:
                    raise WrongAttributeException(["field", "name", "props"], an)
            indices_list.append(idx)

        return indices_list

    @staticmethod
    def get_constraints(table):
        """
        :param      table: Dbc.Table() to find constraints in.

        :return:    constraints_list: list of constraints in table.

        Parse table to find constraints.
        """
        if table.nodeName != "table":
            raise WrongNodeException("table", table.nodeName)

        constraints_list = list()
        for constraint in table.getElementsByTagName("constraint"):
            const = Dbc.Constraint()
            for an, av in constraint.attributes.items():
                if an.lower() == "name":
                    const.name = av
                elif an.lower() == "kind":
                    const.kind = av
                elif an.lower() == "items":
                    const.items = av
                elif an.lower() == "unique_key_id":
                    const.unique_key_id = av
                elif an.lower() == "reference":
                    const.reference = av
                elif an.lower() == "expression":
                    const.expression = av
                elif an.lower() == "props":
                    for prop in av.split(", "):
                        if prop == "has_value_edit":
                            const.has_value_edit = True
                        elif prop == "cascading_delete":
                            const.cascading_delete = True
                        # elif prop == "full_cascading_delete":
                        #     const.full_cascading_delete = True
                        elif prop == "full_cascading_delete":
                            pass
                        else:
                            raise WrongPropertyException(
                                ["has_value_edit", "cascading_delete", "full_cascading_delete"], prop)
                else:
                    raise WrongAttributeException(["name", "constraint_type", "unique_key_id", "expression" "kind",
                                                   "items", "reference_type", "reference", "props"], an)
            constraints_list.append(const)

        return constraints_list

    @staticmethod
    def get_fields(table):
        """
        :param      table: Dbc.Table() to find fields in.

        :return:    fields_list: list of fields in table.

        Parse table to find fields.
        """
        if table.nodeName != "table":
            raise WrongNodeException("table", table.nodeName)

        fields_list = list()
        pos = 1
        for field in table.getElementsByTagName("field"):
            fld = Dbc.Field()
            fld.position = str(pos)
            for an, av in field.attributes.items():
                if an.lower() == "name":
                    fld.name = av
                elif an.lower() == "rname":
                    fld.rname = av
                elif an.lower() == "domain":
                    fld.domain = av
                elif an.lower() == "description":
                    fld.description = av
                elif an.lower() == "props":
                    for prop in av.split(", "):
                        if prop == "input":
                            fld.input = True
                        elif prop == "edit":
                            fld.edit = True
                        elif prop == "show_in_grid":
                            fld.show_in_grid = True
                        elif prop == "show_in_details":
                            fld.show_in_details = True
                        elif prop == "autocalculated":
                            fld.autocalculated = True
                        elif prop == "is_mean":
                            fld.is_mean = True
                        elif prop == "required":
                            fld.required = True
                        else:
                            raise WrongPropertyException(["input", "edit", "show_in_grid",
                                                          "show_in_details", "is_mean", "autocalculated", "required"],
                                                         prop)
                else:
                    raise WrongAttributeException(["name", "rname", "position", "domain", "description", "props"], an)
            pos += 1
            fields_list.append(fld)

        return fields_list

    def get_tables(self):
        """
        :return:    tables_list: list of tables in xml file.

        Parse a xml file to find tables.
        """
        tables_list = list()
        for table in self.xml_repr.getElementsByTagName("table"):
            tbl = Dbc.Table()
            for an, av in table.attributes.items():
                if an.lower() == "name":
                    tbl.name = av
                elif an.lower() == "description":
                    tbl.description = av
                elif an.lower() == "props":
                    for prop in av.split(", "):
                        if prop == "add":
                            tbl.add = True
                        elif prop == "edit":
                            tbl.edit = True
                        elif prop == "delete":
                            tbl.delete = True
                        elif prop == "temporal_mode":
                            tbl.temporal_mode = True
                        else:
                            raise WrongPropertyException(["delete", "temporal_mode", "edit", "add"], prop)
                # elif an.lower() == "ht_table_flags":
                #     tbl.ht_table_flags = av
                # elif an.lower() == "access_level":
                #     tbl.access_level = av
                elif an.lower() == "ht_table_flags":
                    pass
                elif an.lower() == "access_level":
                    pass
                elif an.lower() == "means":
                    tbl.means = av
                else:
                    raise WrongAttributeException(["props", "access_level", "ht_table_flags", "description", "name"],
                                                  an)
            tbl.fields = self.get_fields(table)
            tbl.indices = self.get_indices(table)
            tbl.constraints = self.get_constraints(table)
            tables_list.append(tbl)

        return tables_list

    def get_schema(self):
        """
        :return:    schema: Dbc.Schema() of data base.

        Parse a xml file to obtain schema.
        """
        schema = Dbc.Schema()

        for an, av in self.xml_repr.documentElement.attributes.items():
            # if an.lower() == "fulltext_engine":
            #     schema.fulltext_engine = av
            # elif an.lower() == "version":
            #     schema.version = av
            # elif an.lower() == "name":
            #     schema.name = av
            # elif an.lower() == "description":
            #     schema.description = av
            if an.lower() == "fulltext_engine":
                pass
            elif an.lower() == "version":
                pass
            elif an.lower() == "name":
                schema.name = av
            elif an.lower() == "description":
                pass
            else:
                raise WrongAttributeException(["fulltext_engine", "version", "name", "description"], an)
        schema.domains = self.domain_list
        schema.tables = self.tables_list

        return schema
