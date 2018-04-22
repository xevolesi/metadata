import sqlite3
import data_base_sources.DBClasses as Dbc


class SchemaNameError(ValueError):
    """Error for catching ValueError if we can't find schema with name we need."""


class DBDToRAMTranslator:
    """Translates DBD → RAM."""
    def __init__(self, db_name):
        self.connection = sqlite3.connect(db_name)

    def fetch_schema(self):
        schema = Dbc.Schema()
        cursor = self.connection.cursor()

        # sid, name = cursor.execute("""select id, name from dbd$schemas where name = ?""",
        #                            (self.schema_name,)).fetchone()
        # if name is None:
        #     raise SchemaNameError("Can't find schema with {} name".format(self.schema_name))
        # else:
        #     schema.name = name
        schema.domains = self.fetch_domains()
        schema.tables = self.fetch_tables()
        self.connection.commit()
        self.connection.close()
        return schema

    def fetch_domains(self):
        cursor = self.connection.cursor()

        domain_list = list()
        domain_attributes = cursor.execute("""\
        select name, description, data_type_id, align, width, length, precision, show_null, summable, case_sensitive, \
        show_lead_nulls, thousands_separator, char_length, scale from dbd$domains""").fetchall()
        for attr_tuple in domain_attributes:
            domain = Dbc.Domain()
            domain.name, domain.description, domain.type, domain.align, domain.width, domain.length, \
                domain.precision, domain.show_null, domain.summable, domain.case_sensitive, domain.show_lead_nulls, \
                domain.thousands_separator, domain.char_length, domain.scale = attr_tuple

            domain.show_null, domain.show_lead_nulls, domain.thousands_separator, domain.summable, \
                domain.case_sensitive = map(bool, [domain.show_null, domain.show_lead_nulls, domain.thousands_separator,
                                                   domain.summable, domain.case_sensitive])

            # domain.char_length, domain.length, domain.scale, domain.precision, domain.width = \
            #     map(str, [domain.char_length, domain.length, domain.scale, domain.precision, domain.width])

            domain.char_length = str(domain.char_length) if domain.char_length else None
            domain.length = str(domain.length) if domain.length else None
            domain.scale = str(domain.scale) if domain.scale else None
            domain.precision = str(domain.precision) if domain.precision else None
            domain.width = str(domain.width) if domain.width else None

            domain.type = cursor.execute("""select type_id from dbd$data_types where dbd$data_types.id = ?""",
                                         (domain.type, )).fetchone()[0]
            domain_list.append(domain)

        return domain_list

    def fetch_fields(self, table_id):
        cursor = self.connection.cursor()

        field_list = list()
        filed_attributes = cursor.execute("""\
        select name, russian_short_name, description, domain_id, can_input, can_edit, \
        show_in_grid, show_in_details, is_mean, autocalculated, required from dbd$fields\
        where dbd$fields.table_id = ?""", (table_id,)).fetchall()
        for attr_tuple in filed_attributes:
            field = Dbc.Field()
            field.name, field.rname, field.description, field.domain, field.input, field.edit, \
                field.show_in_grid, field.show_in_details, field.is_mean, field.autocalculated, \
                field.required = attr_tuple
            field.input, field.edit, field.show_in_grid, field.show_in_details, field.is_mean, field.autocalculated, \
                field.required = map(bool, [field.input, field.edit, field.show_in_grid, field.show_in_details,
                                            field.is_mean, field.autocalculated, field.required])
            field.domain = cursor.execute("""select name from dbd$domains where dbd$domains.id = ?""",
                                          (field.domain, )).fetchone()[0]
            field_list.append(field)

        return field_list

    def fetch_constraints(self, table_id):
        cursor = self.connection.cursor()

        constraints_list = list()
        constraints_attributes = cursor.execute("""\
        select id, table_id, name, constraint_type, reference, unique_key_id, has_value_edit, cascading_delete, expression\
        from dbd$constraints\
        where dbd$constraints.table_id = ?""", (table_id,)).fetchall()
        for attr_tuple in constraints_attributes:
            constraint = Dbc.Constraint()
            _, tbl_id, constraint.name, constraint.kind, constraint.reference, constraint.unique_key_id, \
                constraint.has_value_edit, constraint.cascading_delete, constraint.expression = attr_tuple
            constraint.has_value_edit, constraint.cascading_delete = map(bool, [constraint.has_value_edit,
                                                                                constraint.cascading_delete])

            constraint.items = cursor.execute("""\
                        select name from dbd$fields\
                        where dbd$fields.id = (\
                        select field_id from dbd$constraint_details\
                        where dbd$constraint_details.constraint_id = ?)""", (attr_tuple[0],)).fetchone()[0]

            constraint.reference = None if constraint.kind == "PRIMARY" else cursor.execute("""\
            select name from dbd$tables where dbd$tables.id = ?""", (constraint.reference, )).fetchone()[0]
            constraints_list.append(constraint)



        return constraints_list

    def fetch_indices(self, table_id):
        cursor = self.connection.cursor()

        indices_list = list()
        indices_attributes = cursor.execute("""\
        select id, name, local, kind\
        from dbd$indices\
        where dbd$indices.table_id = ?""", (table_id, )).fetchall()
        for attr_tuple in indices_attributes:
            index = Dbc.Index()
            if attr_tuple[3] == "fulltext":
                index.fulltext, index.uniqueness = True, False
            elif attr_tuple[3] == "uniqueness":
                index.fulltext, index.uniqueness = False, True
            else:
                index.fulltext, index.uniqueness = False, False
            index.name, index.local = attr_tuple[1:-1]
            index.field = cursor.execute("""\
            select name from dbd$fields \
            where dbd$fields.id = (\
            select field_id from dbd$index_details\
            where dbd$index_details.index_id = ?)""", (attr_tuple[0], )).fetchone()[0]
            indices_list.append(index)

        return indices_list

    def fetch_tables(self):
        cursor = self.connection.cursor()

        tables_list = list()
        tables_attributes = cursor.execute("""\
        select id, name, description, can_add, can_edit, can_delete, temporal_mode, means from dbd$tables""").fetchall()
        for attr_tuple in tables_attributes:
            table = Dbc.Table()
            tid, table.name, table.description, table.add, table.edit, table.delete, table.temporal_mode, \
                table.means = attr_tuple
            table.add, table.edit, table.delete = map(bool, [table.add, table.edit, table.delete])
            table.fields = self.fetch_fields(tid)
            table.constraints = self.fetch_constraints(tid)
            table.indices = self.fetch_indices(tid)
            tables_list.append(table)

        return tables_list

    def dbd_to_ram(self):
        pass
