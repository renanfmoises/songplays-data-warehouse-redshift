"""This module creates the tables in the Redshift Cluster."""

import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from aws_utils import get_conn_string
from aws_params import get_params
from aws_redshift_cluster import get_redshift_client
from aws_redshift_cluster import describe_cluster


def drop_tables(cur, conn):
    """This function drops the tables in the Redshift Cluster.

    Args:
        cur  (object): cursor to the database
        conn (object): connection to the database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """This function creates the tables in the Redshift Cluster.

    Args:
        cur  (object): cursor to the database
        conn (object): connection to the database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """This function is the main function for creating tables in Redshift Cluster."""
    awsParams = get_params()

    redshift_client = get_redshift_client(
        region_name=awsParams.REGION,
        aws_access_key_id=awsParams.KEY,
        aws_secret_access_key=awsParams.SECRET,
    )

    (_, dwh_endpoint) = describe_cluster(
        redshift_cluster = redshift_client,
        cluster_identifier=awsParams.DWH_CLUSTER_IDENTIFIER,
    )

    conn_string = get_conn_string(
        dwh_db_user = awsParams.DWH_DB_USER,
        dwh_db_password = awsParams.DWH_DB_PASSWORD,
        dwh_endpoint = dwh_endpoint,
        dwh_port = awsParams.DWH_DB_PORT,
        dwh_db_name = awsParams.DWH_DB_NAME,
    )

    conn = psycopg2.connect(conn_string)

    cur = conn.cursor()

    print("Dropping tables if exists...")
    drop_tables(cur, conn)
    print("Creating tables...")
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
