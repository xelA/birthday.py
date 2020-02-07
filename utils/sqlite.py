import sqlite3

from collections import OrderedDict


def dict_factory(cursor, row):
    d = {}
    for index, col in enumerate(cursor.description):
        d[col[0]] = row[index]
    return d


class Database:
    def __init__(self):
        self.conn = sqlite3.connect(
            "storage.db", isolation_level=None, detect_types=sqlite3.PARSE_DECLTYPES
        )
        self.conn.row_factory = dict_factory
        self.db = self.conn.cursor()

    def execute(self, sql: str, prepared: tuple = (), commit: bool = True):
        """ Execute SQL command with args for 'Prepared Statements' """
        try:
            data = self.db.execute(sql, prepared)
        except Exception as e:
            return f"{type(e).__name__}: {e}"

        status_word = sql.split(' ')[0].upper()
        status_code = data.rowcount if data.rowcount > 0 else 0
        if status_word == "SELECT":
            status_code = len(data.fetchall())

        return f"{status_word} {status_code}"

    def fetch(self, sql: str, prepared: tuple = ()):
        """ Fetch DB data with args for 'Prepared Statements' """
        data = self.db.execute(sql, prepared).fetchall()
        return data

    def fetchrow(self, sql: str, prepared: tuple = ()):
        """ Fetch DB row (one row only) with args for 'Prepared Statements' """
        data = self.db.execute(sql, prepared).fetchone()
        return data


class Column:
    def __init__(self, column_type: str, primary_key: bool = False, index: bool = False,
                 nullable: bool = True, unique: bool = False, name: str = None, default=None):
        self.column_type = column_type.upper()
        self.primary_key = primary_key
        self.nullable = nullable
        self.unique = unique
        self.index = index
        self.default = default
        self.name = name

        if sum(map(bool, (unique, primary_key, default is not None))) > 1:
            raise SyntaxError("'unique', 'primary_key', and 'default' are mutually exclusive.")

    def _create_table(self):
        builder = []
        builder.append(f"'{self.name}' {self.column_type}")

        default = self.default
        if default:
            builder.append("DEFAULT")
            if isinstance(default, str):
                builder.append(f"'{default}'")
            elif isinstance(default, bool):
                builder.append(str(default).upper())
            else:
                builder.append(f"({default})")
        elif self.unique:
            builder.append("UNIQUE")
        if not self.nullable:
            builder.append("NOT NULL")

        return " ".join(builder)


class TableMeta(type):
    @classmethod
    def __prepare__(cls, name, bases, **kwargs):
        return OrderedDict()

    def __new__(cls, name, parents, dct, **kwargs):
        columns = []

        try:
            table_name = kwargs["table_name"]
        except KeyError:
            table_name = name.lower()

        dct["__tablename__"] = table_name

        for elem, value in dct.items():
            if isinstance(value, Column):
                if not value.name:
                    value.name = elem

                if value.index:
                    value.index_name = f"{table_name}_{value.name}_idx"

                columns.append(value)

        dct["columns"] = columns
        return super().__new__(cls, name, parents, dct)

    def __init__(self, name, parents, dct, **kwargs):
        super().__init__(name, parents, dct)


class Table(metaclass=TableMeta):
    @classmethod
    def create_table(cls, *, exists_ok: bool = True):
        """ Generate a CREATE TABLE command """
        statements = []
        builder = ["CREATE TABLE"]

        if exists_ok:
            builder.append("IF NOT EXISTS")

        builder.append(cls.__tablename__)
        column_creations = []
        primary_keys = []

        for col in cls.columns:
            column_creations.append(col._create_table())
            if col.primary_key:
                primary_keys.append(col.name)

        column_creations.append("PRIMARY KEY (%s)" % ", ".join(primary_keys))
        builder.append("(%s)" % ", ".join(column_creations))
        statements.append(" ".join(builder) + ";")

        for column in cls.columns:
            if column.index:
                fmt = "CREATE INDEX IF NOT EXISTS {1.index_name} ON {0} ({1.name});".format(cls.__tablename__, column)
                statements.append(fmt)

        return "\n".join(statements)

    @classmethod
    def create(cls, *, verbose: bool = False):
        sql = cls.create_table(exists_ok=True)
        if verbose:
            print(sql)
        Database().execute(sql)
        return True

    @classmethod
    def all_tables(cls):
        return cls.__subclasses__()
