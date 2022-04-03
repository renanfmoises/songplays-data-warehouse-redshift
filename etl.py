"""
This module is responsible for loading the data from the S3 bucket into staging and then
inserting the data into the Redshift Cluster
"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """This function loads the data from the S3 bucket into the staging tables

    Args:
        cur  (object): cursor to the database
        conn (object): connection to the database
    """

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """This function inserts the data from the staging tables into the Redshift Cluster

    Args:
        cur  (object): cursor to the database
        conn (object): connection to the database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """This function is the main function of the ETL pipeline"""
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    db_conn_config = "host={} dbname={} user={} password={} port={}".format(
        *config["CLUSTER"].values()
    )

    conn = psycopg2.connect(db_conn_config)

    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
