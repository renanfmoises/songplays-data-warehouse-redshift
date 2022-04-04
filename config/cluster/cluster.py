import botocore
import psycopg2

import sys
import os

sys.path.append(os.path.abspath(os.path.join("..")))
from config.utils import utils


def authorize_ingress(ec2_client, vpc_id, port, protocol="TCP", cidr_ip="0.0.0.0/0"):
    """
    Open an incoming TCP port to access the cluster ednpoint
    :param ec2_client: A low-level client representing Amazon Elastic Compute
                       Cloud (EC2)
    :param vpc_id: The VPC identifier of the cluster
    :param port: The port to open
    :param protocol:
    :param cidr_ip:
    """
    try:
        vpc = ec2_client.Vpc(id=vpc_id)
        defaultSg = list(vpc.security_groups.all())[0]

        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp=cidr_ip,
            IpProtocol=protocol,
            FromPort=int(port),
            ToPort=int(port),
        )

    except Exception as e:
        print(e)


def create(rs_client, clust_cfg, iam_roles):
    """
    Creates a new cluster with the specified parameters.

    :param rs_client: A low-level client representing Amazon Redshift.

    :param clust_cfg: A config dictionary containing the following keys:
                        CLUSTER_TYPE, NODE_TYPE, NUM_NODES, DB,
                        CLUSTER_IDENTIFIER, DB_USER, DB_PASSWORD

    :param iam_roles: A list of AWS Identity and Access Management (IAM) roles,
                      in  Amazon Resource Name format, that can be used by the
                      cluster to access other AWS services.
    """
    try:
        return rs_client.create_cluster(
            # parameters for hardware
            ClusterType=clust_cfg["CLUSTER_TYPE"],
            NodeType=clust_cfg["NODE_TYPE"],
            NumberOfNodes=int(clust_cfg["NUM_NODES"]),
            # parameters for identifiers & credentials
            DBName=clust_cfg["DB"],
            ClusterIdentifier=clust_cfg["CLUSTER_IDENTIFIER"],
            MasterUsername=clust_cfg["DB_USER"],
            MasterUserPassword=clust_cfg["DB_PASSWORD"],
            # parameter for role (to allow s3 access)
            IamRoles=iam_roles,
        )

    except Exception as e:
        print(e)


def delete(rs_client, clust_id):
    """
    Deletes a previously provisioned cluster.
    :param rs_client: A low-level client representing Amazon Redshift.
    :param clust_id: The identifier of the cluster to be deleted.
    """
    try:
        return rs_client.delete_cluster(
            ClusterIdentifier=clust_id, SkipFinalClusterSnapshot=True
        )

    except rs_client.exceptions.ClusterNotFoundFault:
        return None

    except Exception as e:
        print(e)


def describe(rs_client, cluster_id, print_props=False):
    """
    Returns properties of provisioned clusters. Since we are only working with
    a single cluster, this is a convenience function to return the first
    cluster in the list.

    This function also provides the option to print the clusters properties to
    stdout.

    :param rs_client: A low-level client representing Amazon Redshift.

    :param clust_id: The identifier of the cluster to be deleted.

    :param print_props:
    """
    try:
        cluster_props = rs_client.describe_clusters(ClusterIdentifier=cluster_id)[
            "Clusters"
        ][0]
        if print_props:
            pretty_print_props(cluster_props)
        return cluster_props

    except rs_client.exceptions.ClusterNotFoundFault:
        return None

    except Exception as e:
        print(e)


def get_connection(cfg, endpoint=None):
    """
    Get a connection to the cluster.
    :param cfg: config object for the project
    :param endpoint: endpoint address of the cluster
    """
    if not endpoint:
        endpoint = get_endpoint(utils.get_redshift_client(cfg), cfg.CLUSTER_IDENTIFIER)
    return psycopg2.connect(get_connection_string(endpoint, cfg))


def get_connection_string(endpoint, cfg):
    """
    Build the connection string used to connect to the cluster.
    :param endpoint: endpoint address of the cluster
    :param cfg: config object for the project
    """
    return " ".join(
        [
            f"host={endpoint}",
            f"dbname={cfg.DB}",
            f"user={cfg.DB_USER}",
            f"password={cfg.DB_PASSWORD}",
            f"port={cfg.PORT}",
        ]
    )


def get_endpoint(rs_client, cluster_id):
    """
    Get the endpoint address of the cluster to user for connections.
    :param rs_client: A low-level client representing Amazon Redshift.
    :param clust_id: The identifier of the cluster to be deleted.
    """
    cluster_props = describe(rs_client, cluster_id)
    return cluster_props["Endpoint"]["Address"]


def is_available(rs_client, cluster_id):
    clust_props = describe(rs_client, cluster_id)
    return clust_props and clust_props["ClusterStatus"].lower() == "available"


def pretty_print_props(props):
    import pandas as pd

    pd.set_option("display.max_colwidth", None)
    keys_to_print = [
        "ClusterIdentifier",
        "NodeType",
        "ClusterStatus",
        "MasterUsername",
        "DBName",
        "Endpoint",
        "NumberOfNodes",
        "VpcId",
    ]

    print(
        pd.DataFrame(
            data=[(k, v) for k, v in props.items() if k in keys_to_print],
            columns=["Key", "Value"],
        )
    )
