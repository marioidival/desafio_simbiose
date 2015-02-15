"""Data Model ElasticSearch"""
import uuid
from elasticsearch_dsl import DocType, String, Double


class LoanES(DocType):

    person = String()
    loan_value = Double()

    class Meta:
        index = "simbiose_test"

    def save(self, **kwargs):
        self.id = str(uuid.uuid4())
        return super(LoanES, self).save(**kwargs)
