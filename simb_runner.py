"""
File to get arguments by command line
"""
import click

from simb_datamodel.config_datamodel import run_configs
from simb_daemon import SimbDaemon


@click.group()
def simbiose():
    """Package sync to cassandra and elasticsearch"""
    pass


@simbiose.command()
@click.option("--interval", help="Set interval (in seconds) to verification",
              default=10)
def start(interval):
    """
    Start daemon to verify cassandra and elasticsearch, find by new inserts and
    updates.
    """
    # Run configuration to Cassandra and Elasticsearch Data model
    run_configs()
    simbdaemon = SimbDaemon(interval)
    simbdaemon.run()


if __name__ == "__main__":
    simbiose()
