"""This module contains the function that returns the AWS parameters."""

from collections import namedtuple
import configparser

dwhParams = namedtuple(
    "dwhPrams",
            [
                "KEY",
                "SECRET",
                "REGION",
                "DWH_CLUSTER_IDENTIFIER",
                "DWH_CLUSTER_TYPE",
                "DWH_NODE_TYPE",
                "DWH_NUM_NODES",
                "DWH_DB_NAME",
                "DWH_DB_USER",
                "DWH_DB_PASSWORD",
                "DWH_DB_PORT",
                "IAM_ROLE_NAME",
                "LOG_DATA",
                "LOG_JSONPATH",
                "SONG_DATA",
            ]
)

config = configparser.ConfigParser()
# config.read_file(open('dwh.cfg'))
config.read_file(open('../dwh.cfg'))

_dwh_params = dwhParams(
    KEY                     = config.get("AWS", "KEY"),
    SECRET                  = config.get("AWS", "SECRET"),
    REGION                  = config.get("AWS", "REGION"),
    DWH_CLUSTER_IDENTIFIER  = config.get("DWH", "DWH_CLUSTER_IDENTIFIER"),
    DWH_CLUSTER_TYPE        = config.get("DWH", "DWH_CLUSTER_TYPE"),
    DWH_NODE_TYPE           = config.get("DWH", "DWH_NODE_TYPE"),
    DWH_NUM_NODES           = config.get("DWH", "DWH_NUM_NODES"),
    DWH_DB_NAME             = config.get("DWH", "DWH_DB_NAME"),
    DWH_DB_USER             = config.get("DWH", "DWH_DB_USER"),
    DWH_DB_PASSWORD         = config.get("DWH", "DWH_DB_PASSWORD"),
    DWH_DB_PORT             = config.get("DWH", "DWH_DB_PORT"),
    IAM_ROLE_NAME           = config.get("IAM_ROLE", "IAM_ROLE_NAME"),
    LOG_DATA                = config.get("S3", "LOG_DATA"),
    LOG_JSONPATH            = config.get("S3", "LOG_JSONPATH"),
    SONG_DATA               = config.get("S3", "SONG_DATA"),
)


def get_params():
    """ This function returns a named tuple of the DWH parameters, which are taken
        from the dwh.cfg file.

        This function assumes that there is a file named dwh.cfg in current directory.
        The config file is expected to have the following sections and keys:

        Section: [AWS]
        Key    :    KEY
                    SECRET
                    REGION

        Section: [DWH]
        Key    :    DWH_CLUSTER_IDENTIFIER
                    DWH_CLUSER_TYPE
                    DHW_NODE_TYPE
                    DWH_NUM_NODES
                    DWH_DB_NAME
                    DWH_DB_USER
                    DWH_DB_PASSWORD
                    DWH_DB_PORT

        Section: [IAM_ROLE]
        Key    :    IAM_ROLE_NAME

        Section: [S3]
        Key    :    LOG_DATA
                    LOG_JSONPATH
                    SONG_DATA

    Returns:
        namedtuple: A named tuple of the DWH parameters.
    """
    return _dwh_params