"""
    File to config Cassandra and ElasticSearch data models
"""
from cqlengine.management import sync_table, create_keyspace
from cqlengine.connection import setup
from elasticsearch_dsl.connections import connections

from simb_cassandra import Loan as LoanCassandra
from simb_elasticsearch import Loan as LoanES


def config_cassandra():
    """
    Method to startup cqlengine data model
    """
    keyspace_name = "simbiose_test"

    # setup cassandra and set default keyspaces to models
    setup(['localhost'], keyspace_name)

    create_keyspace(keyspace_name, replication_factor=1,
                    strategy_class="SimpleStrategy")

    # Create tables to keyspaces
    sync_table(LoanCassandra)


def config_elasticsearch():
    """
    Startup elasticsearch modelsh
    """
    connections.create_connection(hosts=['localhost'])

    # Create index
    LoanES.init()


def run_configs():
    config_cassandra()
    config_elasticsearch()
