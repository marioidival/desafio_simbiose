"""
File to get arguments by command line
"""
import click

from simb_datamodel.config_datamodel import run_configs


@click.group
def simbose():
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
    pass


@simbiose.command()
@click.argument("stop")
def stop(stop):
    """Stop daemon verification"""
    pass
