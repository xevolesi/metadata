import itertools
from collections import Counter
# TYPES = ("LARGEINT",
#          "DATE",
#          "CODE",
#          "BYTE",
#          "TIME",
#          "BLOB",
#          "SMALLINT",
#          "MEMO",
#          "WORD",
#          "BOOLEAN")
# TYPES_WITH_LENGTH = ("STRING", "CODE")
# TYPES_WITH_PRECISION_AND_SCALE = ("FLOAT",)
TYPES = dict(
        INTEGER="int",
        LARGEINT="bigint",
        DATE="date",
        BYTE="smallint",
        TIME="time",
        BLOB="bytea",
        SMALLINT="smallint",
        MEMO="text",
        WORD="smallint",
        BOOLEAN="boolean",
        DATETIME="date",
        CURRENCY="money")
TYPES_WITH_PRECISION_AND_SCALE = dict(
        FLOAT="numeric")
TYPES_WITH_LENGTH = dict(
        STRING="varchar",
        CODE="varchar")


class DomainLengthNotFoundError(ValueError):
    """Thrown if we find domain that should has length or char_length but it has not."""


class PrecisionOrScaleNotFoundError(ValueError):
    """Thrown if we find domain that should has precision or scale but it has not."""


class RAMToPostgreSQLTranslator:
    """Translates RAM representation into PostgreSQL DDL."""
    def __init__(self, ram_representation, schema_name):
        self.ram_repr = ram_representation
        self.schema_name = schema_name

    def ram_to_postgresql(self):
        domains = []
        tables = []
        indices = []
        pks = []
        constraints = []
        for domain in self.ram_repr.domains:
            domains.append(self.create_domain(domain))
        for table in self.ram_repr.tables:
            tbl, indices_, pks_, constraints_ = self.create_table(table)
            tables.append(tbl)
            indices.extend(indices_)
            pks.extend(pks_) if type(pks_) != str else pks.append(pks_)
            constraints.extend(constraints_)
        return "\n".join([
            "begin transaction;",
            "set constraints all deferred;",
            self.create_schema(),
            ";".join([";\n".join(itertools.chain(domains, tables, indices)), "\n"]),
            ";".join([";\n".join(pks), "\n"]),
            ";".join([";\n".join(constraints), "\n"]),
            "commit;"])

    def create_schema(self):
        return """drop schema if exists {schema_name} cascade;\ncreate schema {schema_name};""".\
            format(schema_name=self.schema_name)

    def create_domain(self, domain):
        return """create domain {schema_name}.{domain_name} as {type_name}""".format(
            schema_name=self.schema_name,
            domain_name="".join([symbol for symbol in domain.name if symbol not in "\/.- "])
            if domain.name is not None else "",
            type_name=get_domain_type(domain))

    def create_field(self, field):
        # print(field.domain)
        return """{fld_name} {type}""".format(
            fld_name=field.name,
            type=get_domain_type(list(filter((lambda x: x.name == field.domain), self.ram_repr.domains))[0])
            if type(field.domain) == str else get_domain_type(field.domain))

    def create_index(self, index, table):
        table_name = "".join([symbol for symbol in table.name if symbol not in "\/.- "])

        return """create {unique} index {name} on {schema_name}.{tableName} ({field})""".format(
            unique="unique" if index.uniqueness else "",
            schema_name=self.schema_name,
            name="",    # index.name if index.name is not None else "",
            tableName=table_name,
            field=index.field if not index.uniqueness else
            ", ".join([index.field for index in table.indices if index.uniqueness]))

    def create_constraint(self, constraint, table):
        base_query = """alter table {schema_name}.{table_name}\nadd {constraint_name} {constraint_type} ({item})"""
        addition_query = """references {schema_name}.{reference}"""
        repl_name = ""
        for fld in table.fields:
            if constraint.kind == "CHECK" and fld.name in constraint.expression and fld.domain.type == "CURRENCY":
                repl_name = fld.name
        return (base_query if constraint.kind != "FOREIGN" else "\n".join([base_query, addition_query])).format(
            schema_name=self.schema_name,
            table_name="".join([symbol for symbol in table.name if symbol not in "\/.- "]),
            constraint_name="",     # "" ".join(["constraint", constraint.name]) if constraint.name is not None else "",
            constraint_type=constraint.kind if constraint.kind == "CHECK" else " ".join([constraint.kind, "key"]),
            item=constraint.items
            if constraint.kind != "CHECK" else "".join(symbol for symbol in constraint.expression if symbol
                                                       not in "[]").replace("getdate()", "current_date").\
            replace(repl_name, "".join([repl_name, "::money::numeric::float8"])) if repl_name else
            "".join(symbol for symbol in constraint.expression if symbol not in "[]").replace("getdate()",
                                                                                              "current_date"),
            reference=constraint.reference)

    @staticmethod
    def has_complex_pk(table):
        return True if len([constraint for constraint in table.constraints if constraint.kind == 'PRIMARY']) > 1 \
            else False

    def create_complex_pk(self, table):
        return """alter table {schema_name}.{table_name}\nadd {constraint_type} ({items})""".format(
            schema_name=self.schema_name,
            table_name="".join([symbol for symbol in table.name if symbol not in "\/.- "]),
            constraint_type="PRIMARY KEY",
            items=", ".join([constraint.items for constraint in table.constraints if constraint.kind == "PRIMARY"]))

    def create_table(self, table):
        fields = ",\n".join(map(lambda f: self.create_field(f), table.fields))
        indices = map(lambda i: self.create_index(i, table), table.indices)
        pks = map(lambda c: self.create_constraint(c, table),
                  filter(lambda c: c.kind == "PRIMARY", table.constraints)) if not self.has_complex_pk(table) \
            else self.create_complex_pk(table)
        constraints = map(lambda c: self.create_constraint(c, table),
                          filter(lambda c: c.kind != "PRIMARY", table.constraints))
        table_name = "".join([symbol for symbol in table.name if symbol not in "\/.- "])
        return """create table {schema_name}.{table_name}(\n{fields}\n)""".format(
            schema_name=self.schema_name,
            table_name=table_name,
            fields=fields), \
            list(indices), list(pks) if type(pks) == map else pks, list(constraints)


def get_domain_type(domain):
    if domain.type in TYPES:
        return TYPES[domain.type]
    if domain.type in TYPES_WITH_LENGTH:
        if not(domain.length or domain.char_length):
            raise DomainLengthNotFoundError(
                "Got domain type {} that should has length or char length, but it has not".format(domain.type))
        else:
            return "{}({})".format(TYPES_WITH_LENGTH[domain.type],
                                   domain.length if domain.length else domain.char_length)
    if domain.type in TYPES_WITH_PRECISION_AND_SCALE:
        if domain.precision is not None:
            precision = domain.precision
            if domain.scale is not None:
                precision = ", ".join([precision, domain.scale])
        else:
            raise PrecisionOrScaleNotFoundError(
                "Got domain type {} that should has precision or scale, but it has not.".format(domain.type))
        return "{}({})".format(TYPES_WITH_PRECISION_AND_SCALE[domain.type], precision if precision else "")
    return "text"
