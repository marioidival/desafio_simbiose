"""
File to run daemon task

    internal usage:
        sched = BackgroundScheduler(daemon=True)

        sched.add_job(job_method, "interval", seconds=seconds)

        sched.start()
"""
from apscheduler.schedulers.background import BackgroundScheduler
from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch


class HandlerCassandra(object):

    def __init__(self):
        self.keyspace = "simbiose_test"
        cass = Cluster()

        self.session = cass.connect(self.keyspace)
        space_info = cass.metadata.keyspaces[self.keyspace]

        self.tables = space_info.tables
        self.table_ids = self.get_ids_from_tables()

    def get_ids_from_tables(self):
        """Return all ids from tables.
        return dict:
            {table: [list_ids]}
        """
        ids = {}

        for table in self.tables:
            ids.setdefault(table, [])
            query = "SELECT id FROM {}".format(table)

            res = self.session.execute(query)
            ids[table] = [r.id for r in res]

        return ids


class HandlerES(object):

    def __init__(self):
        self.es = Elasticsearch()
        self.index = "simbiose_test"
        self.doc_ids = self.get_ids_from_docs()
        self.types_from_index = self.doc_ids.keys()

    def get_ids_from_docs(self):
        """Return all ids from docs.
        return dict:
            {type: [list_ids]}
        """
        res = self.es.search(index=self.index)
        hits = res["hits"]["hits"]
        ids = {}

        for hit in hits:
            ids.setdefault(hit["_type"], [])
            ids[hit["_type"]].append(hit["_id"])

        return ids


class SimbDaemon(object):

    def __init__(self):
        pass

    def save_in_cassandra():
        pass

    def save_in_es():
        pass
