"""
File to run daemon task
"""
import uuid
import time
import copy
from dateutil.parser import parse

from simb_datamodel.simb_cassandra import types_cassandra

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_ALL
from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch


class HandlerCassandra(object):

    def __init__(self):
        self.keyspace = "simbiose_test"
        cass = Cluster()

        self.session = cass.connect(self.keyspace)
        self.space_info = cass.metadata.keyspaces[self.keyspace]

        self.tables = self.space_info.tables
        self.table_ids = self.get_ids_from_tables()

    def get_ids_from_tables(self):
        """Return all ids from tables.
        return dict:
            {table: [list_ids]}
        """
        ids = {}

        for table in self.tables:
            ids.setdefault(table, set())
            query = "SELECT id FROM {0}".format(table)

            res = self.session.execute(query)
            ids[table] = {r.id for r in res}

        return ids

    def create_tables(self, table_items):
        """Create tables in keyspace of Cassandra.
        Define by default table_id name as primary_key and uuid type.

        params:
            dict - table_items, {table_name: {_id: uuid, _source(dict)}}
        """
        create_items = copy.deepcopy(table_items)

        for table, list_values in create_items.items():
            # Get first element of list, because only need of keys (columns),
            # values here not is important
            values = list_values[0]
            column_name = table
            row = ["id uuid PRIMARY KEY"]

            del values["_id"] # remove id to not mapping
            for key, val in values.items():
                row.extend(
                    ["{0} {1}".format(key, types_cassandra[type(val)])]
                )

            fields = " ,".join(row)

            create_query = "CREATE TABLE IF NOT EXISTS {0}.{1} ({2})"\
                    .format(self.keyspace, table, fields)
            res = self.session.execute(create_query)

    def verify_new_columns(self, table, columns):
        """Verify if exists new columns from Elasticsearch in Cassandra.
        If True, Update table in Cassandra"""
        set_columns_es = set(columns)

        current_table = self.space_info.tables[table]
        ct_columns = set(current_table.columns)

        new_columns = set_columns_es - ct_columns

        if not new_columns:
            return False
        else:
            for new_column in new_columns:
                query = "ALTER TABLE {0}.{1} ADD {2} {3}".format(
                    self.keyspace, table, new_column,
                    types_cassandra[type(columns[new_column])]
                )
                self.session.execute(query)
        return True

    def save_in(self, data):
        """Method to save data from ElasticSearch"""
        cdata = copy.deepcopy(data)

        row_aux = []
        row_val = []
        query_column = ""
        query_val = ""
        query = False

        for table, list_values in cdata.items():
            for source in list_values:
                # Change key _id to id
                source["id"] = uuid.UUID(source["_id"])
                del source["_id"]

                self.verify_new_columns(table, source)

                for k in source:
                    row_aux.extend(["{0}".format(k)])
                    row_val.extend(["%({0})s".format(k)])

                query_column = " ,".join(row_aux)
                query_val = " ,".join(row_val)

                query = "INSERT INTO {0}.{1} ({2}) VALUES ({3})"\
                        .format(self.keyspace, table, query_column, query_val)
                self.session.execute(query, source)
            return True

        return False

    def data_from_id(self, table, _id):
        """Get data of table with _id"""
        query = "SELECT * FROM {0} where id={1}".format(table, _id)
        result = self.session.execute(query)
        return result[0]


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
            ids.setdefault(hit["_type"], set())
            ids[hit["_type"]].add(hit["_id"])

        return ids

    def save_in(self, data):
        """Method to save data from Cassandra"""
        for doc, values in data.items():
            for row in values:
                row = row.__dict__
                row_id = row['id']
                del row['id']

                self.es.index(
                    index=self.index, doc_type=doc, id=row_id, body=row
                )

    def data_from_id(self, doc, _id):
        """Get data of doc with _id"""
        res = self.es.get(index=self.index, doc_type=doc, id=_id)
        res["_source"]["created_at"] = parse(
            res["_source"]["created_at"].split()[0]
        )

        return res["_source"]


class SimbDaemon(object):

    def __init__(self, interval):
        self.interval = interval
        self.sched = BackgroundScheduler(daemon=True)

    def daemon_job(self):
        """Get Cassandra and ElasticSearch data. Compare the tables/type and
        ids of both and then merge."""
        self.h_cass = HandlerCassandra()
        self.h_es = HandlerES()

        self.h_es.es.indices.refresh()

        # Getting actual data from both databases
        cass_table_ids = self.h_cass.table_ids
        es_doc_ids = self.h_es.doc_ids

        for table in cass_table_ids:
            cass_table_ids[table] = set(map(str, cass_table_ids[table]))

        for doc in es_doc_ids:
            es_doc_ids[doc] = set(map(str, es_doc_ids[doc]))

        new_docs = {}
        new_tables = {}

        # Getting diferences from both databases
        # Elasticsearch - Cassandra
        for doc in es_doc_ids:
            if doc not in cass_table_ids:
                new_docs[doc] = es_doc_ids[doc]
                new_ids = None
            else:
                new_ids = es_doc_ids[doc] - cass_table_ids[doc]

            if new_ids:
                new_docs[doc] = new_ids

        # Cassandra - Elasticsearch
        for table in cass_table_ids:
            if table not in es_doc_ids:
                new_tables[table] = cass_table_ids[table]
                new_ids = None
            else:
                new_ids = cass_table_ids[table] - es_doc_ids[table]

            if new_ids:
                new_tables[table] = new_ids

        if new_docs:
            self.elasticsearch_to_cassandra(new_docs)
            print "Adding in Cassandra:"
            for doc in new_docs:
                print "\ttable: {0} {1} news records".format(
                    doc, len(new_docs[doc])
                )

        if new_tables:
            self.cassandra_to_elasticsearch(new_tables)
            print "Adding in ElasticSearch"
            for table in new_tables:
                print "\tDoc Type: {0} {1} news records".format(
                    table, len(new_tables[table])
                )

    def prepare_to_sync(self, to_sync, come_to):
        """Prepare an dict aux to save data in cassandra or elasticsearch.
        Returns a dictionary with the table / type as key and the values that
        need to be passed to the database.

        to_sync:
            {table: [ids]}

        come_to:
            string "cass|es"

        """
        items_prepared = {}

        for doc, ids in to_sync.items():
            items_prepared.setdefault(doc, [])
            doc_items = []

            for _id in ids:
                # Return data from specific _id
                if come_to == "cass":
                    data_from_es = self.h_es.data_from_id(doc, _id)
                    data_from_es.update(_id=_id)
                    doc_items.append(data_from_es)
                else:
                    data_from_cass = self.h_cass.data_from_id(doc, _id)
                    doc_items.append(data_from_cass)


            # {table/type: {id: data}}
            items_prepared[doc] = doc_items

        return items_prepared

    def elasticsearch_to_cassandra(self, sync_to_cassandra):
        """Create tables if need and save data come from elasticsearch."""
        items_table = self.prepare_to_sync(sync_to_cassandra, "cass")

        # Create tables to cassandra
        self.h_cass.create_tables(items_table)

        # Save data in Cassandra
        result = self.h_cass.save_in(items_table)

    def cassandra_to_elasticsearch(self, sync_to_elasticsearch):
        """Save data como from Cassandra"""
        items_doc =self.prepare_to_sync(sync_to_elasticsearch, "es")

        result = self.h_es.save_in(items_doc)

    def listener(self, event):
        if event.exception:
            print event.traceback
            print "Job crashed"
        else:
            print "Job worked"

    def run(self):
        self.sched.add_listener(self.listener, EVENT_ALL)
        self.sched.add_job(self.daemon_job, 'interval', seconds=self.interval,
                      id="sycn_job")
        self.sched.start()
        try:
            while True:
                time.sleep(self.interval)
        except KeyboardInterrupt:
            self.sched.shutdown()
