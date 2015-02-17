"""Data Model ElasticSearch"""
import uuid
from elasticsearch_dsl import DocType, String, Double


class Loan(DocType):
    """Data Model to Loan Doc Type in ElasticSearch"""

    person = String()
    loan_value = Double()

    class Meta:
        index = "simbiose_test"

    def save(self, **kwargs):
        """Define id as uuid4"""
        self.id = str(uuid.uuid4())
        return super(LoanES, self).save(**kwargs)
