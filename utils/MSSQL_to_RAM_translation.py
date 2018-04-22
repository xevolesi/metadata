import data_base_sources.DBClasses as Dbc


TYPES = dict(
        bit="BOOLEAN",
        datetime="DATETIME",
        image="BLOB",
        int="INTEGER",
        money="CURRENCY",
        nchar="STRING",
        ntext="MEMO",
        nvarchar="STRING",
        real="FLOAT",
        smallint="SMALLINT",
        varbinary="BLOB",
        varchar="STRING",
        sysname="STRING"
        )


class MSSQLToRAMTranslator:
    def __init__(self, connection):
        self.connection = connection

    def mssql_to_ram(self):
        return self.fetch_schema()

    def fetch_schema(self):
        schema = Dbc.Schema()
        schema.name = "dbo"

        schema.tables = self.fetch_tables(schema)

        # rsd = list()
        # for domain in schema.domains:
        #     if all([domain != dom for dom in rsd]):
        #         rsd.append(domain)
        # schema.domains = rsd

        return schema

    def fetch_indices(self, schema_name, table_name):
        cursor = self.connection.cursor()

        indices_list = list()
        indices = cursor.execute("""
        select 
            ind.name as index_name, 
            ind.is_unique, 
            fti.object_id as is_fulltext, 
            c.name as field_name
        from sys.indexes as ind
        left join sys.fulltext_indexes as fti
        on ind.object_id = fti.object_id
        join sys.index_columns as ic
        on ind.object_id = ic.object_id and ind.index_id = ic.index_id
        join sys.columns as c
        on ind.object_id = c.object_id and ic.column_id = c.column_id
        where ind.object_id = OBJECT_ID(?);""",
                                 ("{schema_name}.{table_name}".format(schema_name=schema_name,
                                                                      table_name=table_name),)).fetchall()
        for ind in indices:
            index = Dbc.Index()
            index.name = ind[0]
            index.uniqueness = ind[1]
            index.fulltext = bool(ind[2])
            index.field = ind[3]
            indices_list.append(index)

        return indices_list

    def fetch_fields(self, table_name, schema):
        cursor = self.connection.cursor()

        field_list = list()
        fields = cursor.execute("""
        select 
            COLUMN_NAME as name, 
            t.name as dom,
            ORDINAL_POSITION as position, 
            sc.is_identity as edit,
            sc.is_hidden as show_in_grid,
            sc.is_computed as autocalculated,
            sc.is_nullable as required,
            col.DATA_TYPE,
            sc.scale,
            sc.precision,
            sc.max_length
        from INFORMATION_SCHEMA.TABLES as tbl
        left join INFORMATION_SCHEMA.COLUMNS as col
        on col.TABLE_NAME = tbl.TABLE_NAME
        left join sys.columns as sc
        on sc.object_id = object_id(tbl.table_schema + '.' + tbl.table_name) and sc.NAME = col.COLUMN_NAME
        left join sys.types as t
        on col.DATA_TYPE = t.name
        where tbl.TABLE_NAME = ?;""", (table_name,)).fetchall()

        # НЕ РАБОТАЕТ (SQL type - 150 not supported yet)
        # prop.value as descript,
        # left join sys.extended_properties prop
        # on prop.major_id = sc.object_id and prop.minor_id = sc.column_id and prop.NAME = 'MS_Description'

        domain_list = list()
        for fld in fields:
            field = Dbc.Field()
            field.name = fld[0]
            field.domain = fld[1]
            field.position = str(fld[2])
            field.edit = fld[3]
            field.show_in_grid = fld[4]
            field.autocalculated = fld[5]
            field.required = fld[6]
            field.input = not(field.autocalculated or field.edit)
            domain = Dbc.Domain()
            domain.char_length = str(fld[10])
            domain.precision = str(fld[9])
            domain.scale = str(fld[8])
            domain.type = TYPES[fld[7]]
            domain_list.append(domain)

            field.domain = domain
            field_list.append(field)
        # schema.domains += domain_list
        return field_list

    def fetch_constraints(self, schema_name, table_name):
        cursor = self.connection.cursor()

        get_primary_keys = cursor.execute("""
        select 
            kc.name, 
            KCU.COLUMN_NAME as items, 
            kc.unique_index_id as unique_key_index
        from sys.tables as t
        join sys.key_constraints as kc
        on t.object_id = kc.parent_object_id
        join INFORMATION_SCHEMA.KEY_COLUMN_USAGE as KCU
        on KCU.CONSTRAINT_NAME = kc.name
        where t.object_id = object_id(?);""", (".".join([schema_name, table_name]))).fetchall()
        primary_keys = list()
        if get_primary_keys:
            for key in get_primary_keys:
                constraint = Dbc.Constraint()
                constraint.name = key[0]
                constraint.kind = "PRIMARY"
                constraint.items = key[1]
                constraint.unique_key_id = str(key[2])
                primary_keys.append(constraint)

        get_foreign_keys = cursor.execute("""
        select 
            fk.name, 
            ac.name, 
            tt.name, 
            fk.delete_referential_action
        from sys.tables as t
        join sys.all_columns as ac
        on t.object_id = ac.object_id
        join sys.foreign_key_columns as fkc
        on ac.column_id = fkc.parent_column_id and t.object_id = fkc.parent_object_id
        join sys.foreign_keys as fk
        on fkc.constraint_object_id = fk.object_id
        join sys.tables as tt
        on tt.object_id = fk.referenced_object_id
        where t.object_id = object_id(?);""", (".".join([schema_name, table_name]))).fetchall()
        foreign_keys = list()
        if get_foreign_keys:
            for key in get_foreign_keys:
                constraint = Dbc.Constraint()
                constraint.name = key[0]
                constraint.kind = "FOREIGN"
                constraint.items = key[1]
                constraint.reference = key[2]
                constraint.cascading_delete = bool(key[3])
                foreign_keys.append(constraint)

        get_check_constraints = cursor.execute("""
        select 
            cc.name, 
            ac.name as items, 
            cc.definition as expression
        from sys.tables as t
        join sys.all_columns as ac
        on t.object_id = ac.object_id
        join sys.check_constraints as cc
        on ac.column_id = cc.parent_column_id and t.object_id = cc.parent_object_id
        where t.object_id = object_id(?);""", (".".join([schema_name, table_name]))).fetchall()
        check_constraints = list()
        if get_check_constraints:
            for key in get_check_constraints:
                constraint = Dbc.Constraint()
                constraint.name = key[0]
                constraint.kind = "CHECK"
                constraint.items = key[1]
                constraint.expression = key[2]
                check_constraints.append(constraint)

        return primary_keys + foreign_keys + check_constraints

    def fetch_tables(self, schema):
        cursor = self.connection.cursor()

        tables_list = list()
        tables = cursor.execute("""
        select 
            t.name, 
            OBJECTPROPERTY(t.object_id, 'HasInsertTrigger') as addition, 
            OBJECTPROPERTY(t.object_id, 'HasUpdateTrigger') as edition, 
            t.temporal_type
        from sys.tables as t
        join sys.schemas as s
        on t.schema_id = s.schema_id
        where s.name = ?;""", (schema.name,)).fetchall()
        for tbl in tables:
            table = Dbc.Table()
            table.name = tbl[0]
            table.add = bool(tbl[1])
            table.edit = bool(tbl[2])
            table.temporal_mode = bool(tbl[3])
            table.indices = self.fetch_indices(schema.name, table.name)
            table.constraints = self.fetch_constraints(schema.name, table.name)
            table.fields = self.fetch_fields(table.name, schema)
            tables_list.append(table)

        return tables_list

    # def fetch_domains(self, schema_name, table_name):
    #     cursor = self.connection.cursor()
    #
    #     domain_list = list()
    #     domains = cursor.execute("""
    #     select distinct ac.name as col,
    #                     ac.max_length as char_length,
    #                     ac.precision as prec,
    #                     ac.scale as scale,
    #                     t.name as dom,
    #                     isc.DATA_TYPE
    #     from sys.all_columns as ac
    #     join sys.types as t
    #     on ac.user_type_id = t.user_type_id
    #     join INFORMATION_SCHEMA.COLUMNS as isc
    #     on ac.name = isc.COLUMN_NAME and ac.column_id = isc.ORDINAL_POSITION
    #     where ac.object_id = OBJECT_ID(?);""", (".".join([schema_name, table_name]),)).fetchall()
    #     for dom in domains:
    #         domain = Dbc.Domain()
    #         domain.char_length = str(dom[1])
    #         domain.precision = str(dom[2])
    #         domain.scale = str(dom[3])
    #         domain.name = dom[4]
    #         domain.type = TYPES[dom[5]]
    #         domain_list.append(domain)
    #
    #     return domain_list
