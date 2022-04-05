"""
This module is responsible for loading the data from the S3 bucket into staging and then
inserting the data into the Redshift Cluster
"""

import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from aws_utils import get_conn_string
from aws_params import get_params
from aws_redshift_cluster import get_redshift_client
from aws_redshift_cluster import describe_cluster


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

    print("Loading staging tables...")
    load_staging_tables(cur, conn)
    print("Inserting into tables...")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
