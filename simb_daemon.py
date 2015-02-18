"""
File to run daemon task
"""
import uuid
import time
import copy
import datetime
from dateutil.parser import parse
from dateutil import tz

from simb_datamodel.simb_cassandra import types_cassandra

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_ALL
from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch

HERE = tz.tzlocal()
UTC = tz.gettz('UTC')


class HandlerCassandra(object):

    def __init__(self):
        self.keyspace = "simbiose_test"
        self.cass = Cluster()

        self.session = self.cass.connect(self.keyspace)
        self.space_info = self.cass.metadata.keyspaces[self.keyspace]

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

            del values["id"] # remove id to not mapping
            for key, val in values.items():
                row.extend(
                    ["{0} {1}".format(key, types_cassandra[type(val)])]
                )

            fields = ", ".join(row)

            create_query = "CREATE TABLE IF NOT EXISTS {0}.{1} ({2})"\
                    .format(self.keyspace, table, fields)
            self.session.execute(create_query)

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

    def save_in(self, data, update=True):
        """Method to save data from ElasticSearch"""
        cdata = copy.deepcopy(data)

        query = False

        for table, list_values in cdata.items():
            for source in list_values:
                row_aux = []
                row_val = []
                query_column = ""
                query_val = ""

                # Try change str uuid to UUID type
                try:
                    source["id"] = uuid.UUID(source["id"])
                except:
                    pass

                if not source.get("created_at"):
                    timenow = datetime.datetime.now()
                    source["created_at"] = timenow.replace(microsecond=0)

                if not source.get("updated_at"):
                    timenow = datetime.datetime.now()
                    source["updated_at"] = timenow.replace(microsecond=0)

                # Create new columns if need
                self.verify_new_columns(table, source)

                for k in source:
                    row_aux.extend(["{0}".format(k)])
                    row_val.extend(["%({0})s".format(k)])

                query_column = ", ".join(row_aux)
                query_val = ", ".join(row_val)

                query = "INSERT INTO {0}.{1} ({2}) VALUES ({3})"\
                        .format(self.keyspace, table, query_column, query_val)
                self.session.execute(query, source)
            return True

        return False

    def update_data(self, data):
        """Update table in Cassandra.
        dict data:
            table: data_to_update
        """

        for table, list_values in data.items():
            for source in list_values:
                # if exists new columns, create it.
                self.verify_new_columns(table, source)

                table_id = source["id"]
                set_values = ""
                list_help = ["SET "]

                # No update to id and created_at
                del source["id"]
                del source["created_at"]

                for k, v in source.items():
                    if k in ("updated_at",):
                        # cql type - timestamp
                        list_help.extend(
                            ["{0} = {1}, ".format(
                                k, int(v.strftime("%s")) * 1000
                            )]
                        )
                    else:
                        list_help.extend(["{0} = '{1}', ".format(k, v)])

                # Syntax error in UPDATE
                list_help[-1] = list_help[-1].replace(",", "")
                set_values = ''.join(list_help)

                where = "WHERE id={};".format(table_id)
                query = "UPDATE {0} {1} {2}".format(
                    table, set_values, where
                )
                self.session.execute(query)

    def data_from_id(self, table, _id):
        """Get data of table with _id"""
        query = "SELECT * FROM {0} where id={1}".format(table, _id)
        # Return object Row
        result = self.session.execute(query)
        result = dict(result[0].__dict__)

        # Convert dates in result, Cassandra bug with Python dates
        updated = result["updated_at"]
        newup = updated.replace(tzinfo=UTC)
        newup = newup.astimezone(HERE)

        created = result["created_at"]
        newct = created.replace(tzinfo=UTC)
        newct = newct.astimezone(HERE)

        result["updated_at"] = newup.replace(tzinfo=None)
        result["created_at"] = newct.replace(tzinfo=None)
        return result


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
        try:
            res = self.es.search(
                index=self.index, size=9999, sort=["updated_at", "desc"]
            )
        except:
            res = self.es.search(index=self.index, size=9999)

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
                row_id = row['id']
                del row['id']

                if not row.get('created_at'):
                    timenow = datetime.datetime.now()
                    row['created_at'] = timenow.replace(microsecond=0)

                if not row.get('updated_at'):
                    timenow = datetime.datetime.now()
                    row['updated_at'] = timenow.replace(microsecond=0)

                self.es.index(
                    index=self.index, doc_type=doc, id=row_id, body=row,
                    refresh=True
                )

    def data_from_id(self, doc, _id):
        """Get data of doc with _id"""
        self.es.indices.refresh(index=self.index)

        res = self.es.get(index=self.index, doc_type=doc, id=_id)

        res["_source"]["id"] = uuid.UUID(_id)

        created_at = res["_source"].get("created_at")
        updated_at = res["_source"].get("updated_at")

        if created_at:
            created_at = parse(created_at)
            created_at.replace(microsecond=0)

            res["_source"]["created_at"] = created_at

        if updated_at:
            updated_at = parse(updated_at)
            updated_at.replace(microsecond=0)

            res["_source"]["updated_at"] = updated_at

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

        self.h_cass.cass.refresh_schema("simbiose_test")
        self.h_es.es.indices.refresh(index="simbiose_test")

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
                new_ids = None
                continue
            else:
                new_ids = cass_table_ids[table] - es_doc_ids[table]

            if new_ids:
                new_tables[table] = new_ids

        if new_docs:
            self.elasticsearch_to_cassandra(new_docs)
            print "Adding in Cassandra:"
            for doc in new_docs:
                print "\tTable: {0} {1} news records".format(
                    doc, len(new_docs[doc])
                )

        if new_tables:
            self.cassandra_to_elasticsearch(new_tables)
            print "Adding in ElasticSearch:"
            for table in new_tables:
                print "\tDoc Type: {0} {1} news records".format(
                    table, len(new_tables[table])
                )

        self.verify_updates(cass_table_ids, es_doc_ids)

        if not new_docs and not new_tables:
            print "Nothing to sync"

    def verify_updates(self, table_ids, doc_ids):
        """Verify updates in both databases"""
        # Common names of tables
        cass_tables = set(self.h_cass.tables)
        es_types = set(self.h_es.types_from_index)
        keys = cass_tables.intersection(es_types)

        for k in keys:
            uuids = doc_ids.get(k, set()).union(table_ids.get(k, set()))
            to_es_list = []
            to_cass_list = []

            for uuid in uuids:
                cass_data = self.h_cass.data_from_id(k, uuid)
                es_data = self.h_es.data_from_id(k, uuid)

                if cass_data != es_data:
                    if cass_data["updated_at"] >= es_data["updated_at"]:
                        to_es_list.append(cass_data)
                    else:
                        to_cass_list.append(es_data)

            if to_es_list:
                self.h_es.save_in({k: to_es_list})
                print "Elasticsearch {} Sync {} records".format(
                    k, len(to_es_list)
                )

            if to_cass_list:
                self.h_cass.update_data({k: to_cass_list})
                print "Cassandra {} Sync {} records".format(
                    k, len(to_cass_list)
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
                    data_from_es.update(id=_id)
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
        self.h_cass.save_in(items_table)
        self.h_cass.cass.refresh_schema("simbiose_test")

    def cassandra_to_elasticsearch(self, sync_to_elasticsearch):
        """Save data como from Cassandra"""
        items_doc = self.prepare_to_sync(sync_to_elasticsearch, "es")

        result = self.h_es.save_in(items_doc)
        self.h_es.es.indices.refresh(index="simbiose_test")

    def listener(self, event):
        if event.exception:
            print event.traceback
            print event.exception
            print "Job crashed"

    def run(self):
        self.sched.add_listener(self.listener, EVENT_ALL)
        self.sched.add_job(self.daemon_job, 'interval', seconds=self.interval,
                      id="sycn_job")
        self.sched.start()
        try:
            while True:
                time.sleep(3)
        except KeyboardInterrupt:
            self.sched.shutdown()
