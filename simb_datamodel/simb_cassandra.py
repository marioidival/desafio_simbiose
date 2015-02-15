""" Data Model of cassandra """
import uuid
from cqlengine import columns, Model


class LoanCassandra(Model):

    id = columns.UUID(primary_key=True, default=uuid.uuid4)
    person = columns.Text(required=True)
    loan_value = columns.Decimal(required=True)
