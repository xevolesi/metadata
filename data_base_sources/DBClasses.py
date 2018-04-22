"""Definition of necessary data base classes."""


class Schema:
    """Describes data base schema."""
    def __init__(self):
        # self.fulltext_engine = None
        # self.version = None
        self.name = None
        # self.description = None
        # self.custom = list()
        self.domains = list()
        self.tables = list()


class Table:
    """Describes tables in data base."""
    def __init__(self):
        self.name = None
        self.description = None
        self.temporal_mode = None
        self.add = False
        self.edit = False
        self.delete = False
        # self.ht_table_flags = None
        # self.access_level = None
        self.means = None
        self.fields = list()
        self.constraints = list()
        self.indices = list()


class Field:
    """Describes fields of tables in data base."""
    def __init__(self):
        self.name = None
        self.rname = None
        self.position = None
        self.domain = None
        self.description = None
        self.input = False
        self.edit = False
        self.show_in_grid = False
        self.show_in_details = False
        self.autocalculated = False
        self.is_mean = False
        self.required = False


class Domain:
    """Describes domains in data base."""
    def __init__(self):
        self.name = None
        self.description = None
        self.type = None
        self.align = None
        self.width = None
        self.show_null = False
        self.summable = False
        self.case_sensitive = False
        self.show_lead_nulls = False
        self.thousands_separator = False
        self.char_length = None
        self.length = None
        self.precision = None
        self.scale = None

    # def __eq__(self, other):
    #     return True if all(self.__getattribute__(attr) == other.__getattribute__(attr) for attr in
    #                        filter(lambda x: not x.startswith("_"), dir(self))) else False


class Constraint:
    """Describes table constraints of data base."""
    def __init__(self):
        self.name = None
        self.kind = None                # constraint_type
        self.items = None
        self.unique_key_id = None
        self.reference = None
        self.expression = None
        self.has_value_edit = False
        self.cascading_delete = False
        # self.full_cascading_delete = False


class Index:
    """Describes table indices of data base."""
    def __init__(self):
        self.name = None
        self.field = None
        self.fulltext = False
        self.uniqueness = False
        self.local = False
