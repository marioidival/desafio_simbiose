""" Data Model of cassandra """
import uuid
import decimal
import datetime
from collections import OrderedDict
from cqlengine import columns, Model


class Loan(Model):
    """Data Model to Loan table in Cassandra"""

    id = columns.UUID(primary_key=True, default=uuid.uuid4)
    person = columns.Text(required=True)
    loan_value = columns.Decimal(required=True)

GENERATOR = type((i for i in range(1, 3)))

# Dict with key as python type and values as cql literal type
types_cassandra = {
    None: "NULL",
    bool: "boolean",
    float: "double",
    int: "int",
    long: "varint",
    decimal.Decimal: "decimal",
    str: "varchar",
    unicode: "ascii",
    datetime.date: "timestamp",
    datetime.datetime: "timestamp",
    list: "list",
    tuple: "list",
    GENERATOR: "list",
    set: "set",
    frozenset: "set",
    dict: "map",
    OrderedDict: "map",
    uuid.UUID: "timeuuid",
    uuid.UUID: "uuid",
}
