"""This module manages the Redshift cluster and connections"""

import time
from datetime import datetime
from termcolor import colored
import boto3
from aws_params import get_params
from aws_iam_roles import get_iam_client
from aws_iam_roles import get_role_arn
from aws_utils import get_ec2_resource
from aws_utils import authorize_ingress
from aws_utils import get_conn_string
from aws_utils import test_connection


def get_redshift_client(region_name, aws_access_key_id, aws_secret_access_key):
    """This function returns a boto3 redshift client

    Args:
        region_name (str): AWS region name.
        aws_access_key_id (str): AWS access key id.
        aws_secret_access_key (str): AWS secret access key.

    Returns:
        Redhist client object: The redshift client.
    """
    redshift = boto3.client(
        "redshift",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    return redshift


def create_cluster(
    redshift_client,
    cluster_type,
    node_type,
    number_of_nodes,
    db_name,
    cluster_identifier,
    master_username,
    master_user_password,
    iam_roles,
):
    """This function creates a Redshift cluster on AWS.

    Args:
        redshift_client (Redshift client object): The redshift client.
        cluster_type (str): The cluster type.
        node_type (str): The node type.
        number_of_nodes (str or int): The number of nodes.
        db_name (str): The database name.
        cluster_identifier (str): The cluster identifier.
        master_username (str): The master username.
        master_user_password (str): The master user password.
        iam_roles (IAM client object): The iam roles.
    """
    try:
        response = redshift_client.create_cluster(
            # DHW
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=number_of_nodes,
            # Identifiers & Credentials
            DBName=db_name,
            ClusterIdentifier=cluster_identifier,
            MasterUsername=master_username,
            MasterUserPassword=master_user_password,
            # Roles (for s3 access)
            IamRoles=iam_roles,
        )

    except Exception as e:
        print(e)


def get_cluster_status(redshift_client, cluster_identifier):
    """This function returns the status of a Redshift cluster.

    Args:
        redshift_client (Redshift client object): The Redshift client.
        cluster_identifier (str): The cluster identifier.

    Returns:
        str: The cluster status.
    """
    try:
        response = redshift_client.describe_clusters(
            ClusterIdentifier=cluster_identifier
        )

    except Exception as e:
        print(e)

    else:
        status = response["Clusters"][0]["ClusterStatus"]
        return status


def describe_cluster(redshift_cluster, cluster_identifier):
    """This function describes a Redshift cluster.

    Args:
        redshift_cluster (Redshift client object): The Redshift client.
        cluster_identifier (str): The cluster identifier.

    Returns:
        dict: The cluster description as a Python dictionary.
    """
    try:
        response = redshift_cluster.describe_clusters(
            ClusterIdentifier=cluster_identifier
        )

    except Exception as e:
        print(colored(e, "red"))

    else:
        print(response["Clusters"][0])
        vpc_id = response["Clusters"][0]["VpcId"]
        dwh_endpoint = response["Clusters"][0]["Endpoint"]["Address"]
        # dwh_role_arn =  response['Clusters'][0]['IamRoles'][0]['IamRoleArn']
        return (vpc_id, dwh_endpoint)


def delete_cluster(redshift_client, cluster_identifier):
    """This function deletes a Redshift cluster.

    Args:
        redshift_client (Redshift client object): The Redshift client.
        cluster_identifier (str): The cluster identifier.
    """
    try:
        response = redshift_client.delete_cluster(
            ClusterIdentifier=cluster_identifier, SkipFinalClusterSnapshot=True
        )

    except Exception as e:
        print(e)


def main():
    awsParams = get_params()

    iam_client = get_iam_client(
        region_name=awsParams.REGION,
        aws_access_key_id=awsParams.KEY,
        aws_secret_access_key=awsParams.SECRET,
    )

    role_arn = get_role_arn(iam_client=iam_client, role_name=awsParams.IAM_ROLE_NAME)

    redshift_client = get_redshift_client(
        region_name=awsParams.REGION,
        aws_access_key_id=awsParams.KEY,
        aws_secret_access_key=awsParams.SECRET,
    )

    create_cluster(
        redshift_client=redshift_client,
        cluster_type=awsParams.DWH_CLUSTER_TYPE,
        node_type=awsParams.DWH_NODE_TYPE,
        number_of_nodes=int(awsParams.DWH_NUM_NODES),
        db_name=awsParams.DWH_DB_NAME,
        cluster_identifier=awsParams.DWH_CLUSTER_IDENTIFIER,
        master_username=awsParams.DWH_DB_USER,
        master_user_password=awsParams.DWH_DB_PASSWORD,
        iam_roles=[role_arn],
    )

    status = get_cluster_status(
        redshift_client=redshift_client,
        cluster_identifier=awsParams.DWH_CLUSTER_IDENTIFIER,
    )

    while status != "available":
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        print(f"{dt_string} - CREATING CLUSTER. Cluster is not available yet")
        time.sleep(14)
        status = get_cluster_status(
            redshift_client=redshift_client,
            cluster_identifier=awsParams.DWH_CLUSTER_IDENTIFIER,
        )
    print(f"{dt_string} - CLUSTER CREATED AND AVAILABLE")

    ec2 = get_ec2_resource(
        region_name=awsParams.REGION,
        aws_access_key_id=awsParams.KEY,
        aws_secret_access_key=awsParams.SECRET,
    )

    (vpc_id, dwh_endpoint) = describe_cluster(
        redshift_cluster=redshift_client,
        cluster_identifier=awsParams.DWH_CLUSTER_IDENTIFIER,
    )

    authorize_ingress(ec2=ec2, vpc_id=vpc_id, dwh_port=awsParams.DWH_DB_PORT)

    conn_string = get_conn_string(
        dwh_db_user=awsParams.DWH_DB_USER,
        dwh_db_password=awsParams.DWH_DB_PASSWORD,
        dwh_endpoint=dwh_endpoint,
        dwh_port=awsParams.DWH_DB_PORT,
        dwh_db_name=awsParams.DWH_DB_NAME,
    )

    test_connection(conn_string)


if __name__ == "__main__":
    main()
