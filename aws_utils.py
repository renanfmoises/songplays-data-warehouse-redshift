"""This module contains utility functions for the DWH."""

import boto3
import psycopg2


def get_ec2_resource(region_name, aws_access_key_id, aws_secret_access_key):
    """This function returns an ec2 resource object.

    Args:
        region_name (str): The region where the ec2 resource is located.
        aws_access_key_id (str): The access key id for the ec2 resource.
        aws_secret_access_key (str): The secret access key for the ec2 resource.

    Returns:
        AWS ec2 resource object: The ec2 resource object itself.
    """
    ec2 = boto3.resource(
        "ec2",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    return ec2


def authorize_ingress(ec2, vpc_id, dwh_port):
    """This function authorizes ingress to the dwh_port.

    Args:
        ec2 (ec2 object): The ec2 resource object.
        vpc_id (str): The vpc id.
        dwh_port (str): The port to authorize.
    """
    try:
        vpc = ec2.Vpc(id=vpc_id)
        default_security_group = list(vpc.security_groups.all())[0]

        print("The security group id is: ", default_security_group)

        default_security_group.authorize_ingress(
            GroupName=default_security_group.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(dwh_port),
            ToPort=int(dwh_port),
        )

    except Exception as e:
        print(e)


def get_conn_string(dwh_db_user, dwh_db_password, dwh_endpoint, dwh_port, dwh_db_name):
    """This function returns a connection string.

    Args:
        dwh_db_user (str): The database user.
        dwh_db_password (str): The database password.
        dwh_endpoint (str): The database endpoint.
        dwh_port (str): The database port.
        dwh_db_name (str): The database name.

    Returns:
        str: The connection string.
    """

    conn_string = "postgresql://{}:{}@{}:{}/{}".format(
        dwh_db_user, dwh_db_password, dwh_endpoint, dwh_port, dwh_db_name
    )

    return conn_string


def test_connection(conn_string):
    """This function tests the connection to the database.

    Args:
        conn_string (str): The connection string.

    Returns:
        bool: True if the connection is successful, False otherwise.
    """
    try:
        conn = psycopg2.connect(conn_string)

        if conn is not None:
            conn.close()
            print("Connection successful")
            return True

    except Exception as e:
        print(e)
