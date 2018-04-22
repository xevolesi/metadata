import sqlite3
from itertools import count


class RAMToDBDTranslator:
    """Translates RAM → DBD."""
    def __init__(self, ram_representation, db_name, initial_instruction):
        self.ram_repr = ram_representation
        self.connection = sqlite3.connect(db_name)
        self.init = initial_instruction

    def initialize(self, initial_instruction):
        self.connection.cursor().executescript(initial_instruction)

    def insert_schema(self):
        self.connection.cursor().execute("insert into dbd$schemas (name) values(?)", (self.ram_repr.name,))

    def insert_domains(self):
        cursor = self.connection.cursor()
        cursor.executemany(
            """insert into dbd$domains (
                name,
                description,
                length,
                char_length,
                precision,
                scale,
                width,
                align,
                show_null,
                show_lead_nulls,
                thousands_separator,
                summable,
                case_sensitive,
                data_type_id)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", list((
                    domain.name,
                    domain.description,
                    domain.length,
                    domain.char_length,
                    domain.precision,
                    domain.scale,
                    domain.width,
                    domain.align,
                    domain.show_null,
                    domain.show_lead_nulls,
                    domain.thousands_separator,
                    domain.summable,
                    domain.case_sensitive,
                    "*"
                ) for domain in self.ram_repr.domains))
        cursor.execute(
            "create table dbd$tmp (domain_name varchar not null, domain_type varchar not null, domain_type_id)")
        cursor.executemany(
            "insert into dbd$tmp values (?, ?, ?)", list((domain.name, domain.type, "*")
                                                         for domain in self.ram_repr.domains))
        cursor.execute("""
        update dbd$tmp
        set domain_type_id = (
        select id from dbd$data_types
        where dbd$tmp.domain_type = dbd$data_types.type_id);
        """)
        cursor.execute("""
        update dbd$domains
        set data_type_id = (
        select domain_type_id from dbd$tmp
        where dbd$tmp.domain_name = dbd$domains.name);
        """)
        cursor.execute("drop table dbd$tmp")

    def insert_tables(self):
        cursor = self.connection.cursor()
        cursor.executemany(
            """insert into dbd$tables (
                schema_id,
                name,
                description,
                can_add,
                can_edit,
                can_delete,
                temporal_mode,
                means)
                values (?, ?, ?, ?, ?, ?, ?, ?)""", list((
                    "s_id",
                    table.name,
                    table.description,
                    table.add,
                    table.edit,
                    table.delete,
                    table.temporal_mode,
                    table.means
                ) for table in self.ram_repr.tables))
        # cursor.execute("""
        # update dbd$tables
        # set schema_id = (
        # select id from dbd$schemas
        # where dbd$schemas.name = ?)""", (self.ram_repr.name,))

    def insert_constraints(self):
        cursor = self.connection.cursor()
        for table in self.ram_repr.tables:
            if table.constraints:
                for constraint in table.constraints:
                    tid = cursor.execute("""select id from dbd$tables where dbd$tables.name = ?""",
                                         (table.name,)).fetchone()[0]
                    cursor.execute("""
                    insert into dbd$constraints (
                    table_id,
                    name,
                    constraint_type,
                    reference,
                    unique_key_id,
                    has_value_edit,
                    cascading_delete,
                    expression)
                    values (?, ?, ?, ?, ?, ?, ?, ?)""", (
                        tid,
                        constraint.name,
                        constraint.kind,
                        None if constraint.reference is None else cursor.execute("""select id from dbd$tables \
                         where dbd$tables.name = ?""", (constraint.reference, )).fetchone()[0],
                        constraint.unique_key_id,
                        constraint.has_value_edit,
                        constraint.cascading_delete,
                        constraint.expression))

    def insert_fields(self):
        cursor = self.connection.cursor()
        for table in self.ram_repr.tables:
            if table.fields:
                for field in table.fields:
                    cursor.execute("""
                    insert into dbd$fields (
                    table_id,
                    position,
                    name,
                    russian_short_name,
                    description,
                    domain_id,
                    can_input,
                    can_edit,
                    show_in_grid,
                    show_in_details,
                    is_mean,
                    autocalculated,
                    required)
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
                        cursor.execute("""
                        select id from dbd$tables
                        where dbd$tables.name = ?""", (table.name,)).fetchone()[0],
                        field.position,
                        field.name,
                        field.rname,
                        field.description,
                        cursor.execute("""
                        select id from dbd$domains
                        where dbd$domains.name = ?""", (field.domain,)).fetchone()[0],
                        field.input,
                        field.edit,
                        field.show_in_grid,
                        field.show_in_details,
                        field.is_mean,
                        field.autocalculated,
                        field.required))

    def insert_indices(self):
        cursor = self.connection.cursor()
        for table in self.ram_repr.tables:
            if table.indices:
                for index in table.indices:
                    cursor.execute("""
                    insert into dbd$indices (
                    table_id,
                    name,
                    local,
                    kind)
                    values (?, ?, ?, ?)""", (
                        cursor.execute("""
                        select id from dbd$tables
                        where dbd$tables.name = ?""", (table.name,)).fetchone()[0],
                        index.name,
                        index.local,
                        (lambda: (index.fulltext and "fulltext") or (index.uniqueness and "uniqueness") or
                                 (not(index.fulltext ^ index.uniqueness) and None))()))

    def insert_constraint_details(self):
        constraint_id = count(1)
        cursor = self.connection.cursor()
        for table in self.ram_repr.tables:
            # pos = 1
            for constraint in table.constraints:
                cursor.execute("""
                insert into dbd$constraint_details (
                constraint_id,
                position,
                field_id)
                values (?, ?, ?)""", (
                    next(constraint_id),
                    1,
                    cursor.execute("""
                    select id from dbd$fields
                    where dbd$fields.name = ?""", (constraint.items,)).fetchone()[0]))
                # pos += 1

    def insert_index_details(self):
        index_id = count(1)
        cursor = self.connection.cursor()
        for table in self.ram_repr.tables:
            for index in table.indices:
                cursor.execute("""
                insert into dbd$index_details (
                index_id,
                position,
                field_id,
                expression,
                descend)
                values (?, ?, ?, ?, ?)""", (
                    next(index_id),
                    1,
                    cursor.execute("""
                    select id from dbd$fields
                    where dbd$fields.name = ?""", (index.field,)).fetchone()[0],
                    None,
                    None))

    def ram_to_dbd(self):
        self.initialize(self.init)
        # self.insert_schema()
        self.insert_domains()
        self.insert_tables()
        self.insert_constraints()
        self.insert_fields()
        self.insert_indices()
        self.insert_constraint_details()
        self.insert_index_details()
        self.connection.commit()
        self.connection.close()
