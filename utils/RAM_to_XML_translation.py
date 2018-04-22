import utils.minidom_fixed as mdf


class EmptyFieldError(ValueError):
    """Error for catching ValueError if we can't find field while creating index node during RAM → XML translation."""


class SchemaNotFoundError(ValueError):
    """Error for catching ValueError if we can't find data base schema during RAM → XML translation."""


class RAMToXMLTranslator:
    """Translates RAM → XML."""

    def __init__(self, ram_representation):
        self.ram_repr = ram_representation

    def ram_to_xml(self):
        return self.create_schema()

    def create_schema(self):
        """
        :return:    xml_document: xml.dom.minidom document.

        Creates an xml.dom.minidom document from RAM representation.
        """
        if self.ram_repr is None:
            raise SchemaNotFoundError("Can't find schema!")

        xml_document = mdf.Document()
        n = xml_document.createElement("dbd_schema")
        # if self.ram_repr.fulltext_engine is not None:
        #     n.setAttribute("fulltext_engine", self.ram_repr.fulltext_engine)
        # if self.ram_repr.version is not None:
        #     n.setAttribute("version", self.ram_repr.version)
        if self.ram_repr.name is not None:
            n.setAttribute("name", self.ram_repr.name)
        # if self.ram_repr.description is not None:
        #     n.setAttribute("description", self.ram_repr.description)
        # n.appendChild(xml_document.createElement("custom"))
        domains = xml_document.createElement("domains")
        for domain in self.ram_repr.domains:
            domains.appendChild(self.create_domain(xml_document, domain))
        n.appendChild(domains)
        tables = xml_document.createElement("tables")
        for table in self.ram_repr.tables:
            tables.appendChild(self.create_table(xml_document, table))
        n.appendChild(tables)
        xml_document.appendChild(n)

        return xml_document

    @staticmethod
    def create_domain(xml_document, domain):
        """
        :param      xml_document: xml.dom.minidom document.

        :param      domain: data base domain in RAM representation.

        :return:    node: xml.dom.minidom element representing domain object in xml notation.

        Creates an xml.dom.minidom element "domain" from RAM representation.
        """
        node = xml_document.createElement("domain")

        if domain.name is not None:
            node.setAttribute("name", domain.name)
        if domain.description is not None:
            node.setAttribute("description", domain.description)
        if domain.type is not None:
            node.setAttribute("type", domain.type)
        if domain.align is not None:
            node.setAttribute("align", domain.align)
        if domain.width is not None:
            node.setAttribute("width", domain.width)
        if domain.length is not None:
            node.setAttribute("length", domain.length)
        if domain.precision is not None:
            node.setAttribute("precision", domain.precision)
        props_list = list()
        if domain.show_null:
            props_list.append("show_null")
        if domain.summable:
            props_list.append("summable")
        if domain.case_sensitive:
            props_list.append("case_sensitive")
        if domain.show_lead_nulls:
            props_list.append("show_lead_nulls")
        if domain.thousands_separator:
            props_list.append("thousands_separator")
        if props_list:
            node.setAttribute("props", ", ".join(props_list))
        if domain.char_length is not None:
            node.setAttribute("char_length", domain.char_length)
        if domain.scale is not None:
            node.setAttribute("scale", domain.scale)

        return node

    @staticmethod
    def create_index(xml_document, index):
        """
        :param      xml_document: xml.dom.minidom document.

        :param      index: data base field index in RAM representation.

        :return:    node: xml.dom.minidom element representing index object in xml notation.

        Creates an xml.dom.minidom element "index" from RAM representation.
        """
        node = xml_document.createElement("index")

        if index.field is None:
            raise EmptyFieldError("Can't find field for created index!")
        else:
            node.setAttribute("field", index.field)
            if index.name is not None:
                node.setAttribute("name", index.name)
            props_list = list()
            if index.fulltext:
                props_list.append("fulltext")
            if index.uniqueness:
                props_list.append("uniqueness")
            if index.local:
                props_list.append("local")
            if props_list:
                node.setAttribute("props", ", ".join(props_list))

        return node

    @staticmethod
    def create_constraint(xml_document, constraint):
        """
        :param      xml_document: xml.dom.minidom document.

        :param      constraint: data base field constraint in RAM representation.

        :return:    node: xml.dom.minidom element representing constraint object in xml notation.

        Creates an xml.dom.minidom element "constraint" from RAM representation.
        """
        node = xml_document.createElement("constraint")

        if constraint.name is not None:
            node.setAttribute("name", constraint.name)
        if constraint.kind is not None:
            node.setAttribute("kind", constraint.kind)
        if constraint.items is not None:
            node.setAttribute("items", constraint.items)
        if constraint.unique_key_id is not None:
            node.setAttribute("unique_key_id", constraint.unique_key_id)
        if constraint.reference is not None:
            node.setAttribute("reference", constraint.reference)
        if constraint.expression is not None:
            node.setAttribute("expression", constraint.expression)
        props_list = list()
        if constraint.has_value_edit:
            props_list.append("has_value_edit")
        if constraint.cascading_delete:
            props_list.append("cascading_delete")
        # if constraint.full_cascading_delete:
        #     props_list.append("full_cascading_delete")
        if props_list:
            node.setAttribute("props", ", ".join(props_list))

        return node

    @staticmethod
    def create_field(xml_document, field):
        """
        :param      xml_document: xml.dom.minidom document.

        :param      field: data base table field in RAM representation.

        :return:    node: xml.dom.minidom element representing field object in xml notation.

        Creates an xml.dom.minidom element "field" from RAM representation.
        """
        node = xml_document.createElement("field")

        if field.name is not None:
            node.setAttribute("name", field.name)
        if field.rname is not None:
            node.setAttribute("rname", field.rname)
        # if field.position is not None:
        #     node.setAttribute("position", field.position)
        if field.domain is not None:
            if type(field.domain) == str:
                node.setAttribute("domain", field.domain)
            elif field.domain.name is not None:
                node.setAttribute("domain", field.domain.name)
            else:
                domain_fields = list(filter(lambda x: not x.startswith("_"), dir(field.domain)))
                for domain_field in domain_fields:
                    val = field.domain.__getattribute__(domain_field)
                    if val:
                        node.setAttribute(".".join(["domain", domain_field]), val)
                # for dom_attr in filter(lambda x: not x.startswith("_"), dir(field.domain)):
                #     t = field.domain.__getattribute__(dom_attr)
                #     if t:
                #         node.setAttribute(".".join(["domain", dom_attr]), t)
        if field.description is not None:
            node.setAttribute("description", field.description)
        props_list = list()
        if field.input:
            props_list.append("input")
        if field.edit:
            props_list.append("edit")
        if field.show_in_grid:
            props_list.append("show_in_grid")
        if field.show_in_details:
            props_list.append("show_in_details")
        if field.autocalculated:
            props_list.append("autocalculated")
        if field.is_mean:
            props_list.append("is_mean")
        if field.required:
            props_list.append("required")
        if props_list:
            node.setAttribute("props", ", ".join(props_list))

        return node

    def create_table(self, xml_document, table):
        """
        :param      xml_document: xml.dom.minidom document.

        :param      table: data base table in RAM representation.

        :return:    node: xml.dom.minidom element representing table object in xml notation.

        Creates an xml.dom.minidom element "table" from RAM representation.
        """
        node = xml_document.createElement("table")

        if table.name is not None:
            node.setAttribute("name", table.name)
        if table.description is not None:
            node.setAttribute("description", table.description)
        props_list = list()
        if table.add:
            props_list.append("add")
        if table.edit:
            props_list.append("edit")
        if table.delete:
            props_list.append("delete")
        if table.temporal_mode:
            props_list.append("temporal_mode")
        if props_list:
            node.setAttribute("props", ", ".join(props_list))
        # if table.ht_table_flags is not None:
        #     node.setAttribute("ht_table_flags", table.ht_table_flags)
        # if table.access_level is not None:
        #     node.setAttribute("access_level", table.access_level)
        if table.means is not None:
            node.setAttribute("means", table.means)
        for field in table.fields:
            node.appendChild(self.create_field(xml_document, field))
        for constraint in table.constraints:
            node.appendChild(self.create_constraint(xml_document, constraint))
        for index in table.indices:
            node.appendChild(self.create_index(xml_document, index))

        return node
