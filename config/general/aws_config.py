from collections import namedtuple
import configparser

DWHCfg = namedtuple(
    "DWHCfg",
    [
        "ACCESS_KEY",
        "SECRET_KEY",
        "REGION",
        "CLUSTER_TYPE",
        "NUM_NODES",
        "NODE_TYPE",
        "CLUSTER_IDENTIFIER",
        "DB",
        "DB_USER",
        "DB_PASSWORD",
        "PORT",
        "IAM_ROLE_NAME",
        "LOG_DATA",
        "LOG_JSONPATH",
        "SONG_DATA",
    ],
)

config = configparser.ConfigParser()
config.read_file(open("dwh.cfg"))

_dwh_cfg = DWHCfg(
    ACCESS_KEY=config.get("AWS", "KEY"),
    SECRET_KEY=config.get("AWS", "SECRET"),
    REGION=config.get("AWS", "REGION"),
    CLUSTER_TYPE=config.get("DWH", "CLUSTER_TYPE"),
    NUM_NODES=config.get("DWH", "NUM_NODES"),
    NODE_TYPE=config.get("DWH", "NODE_TYPE"),
    CLUSTER_IDENTIFIER=config.get("DWH", "CLUSTER_IDENTIFIER"),
    DB=config.get("DWH", "DB"),
    DB_USER=config.get("DWH", "DB_USER"),
    DB_PASSWORD=config.get("DWH", "DB_PASSWORD"),
    PORT=config.get("DWH", "PORT"),
    IAM_ROLE_NAME=config.get("DWH", "IAM_ROLE_NAME"),
    LOG_DATA=config.get("S3", "LOG_DATA"),
    LOG_JSONPATH=config.get("S3", "LOG_JSONPATH"),
    SONG_DATA=config.get("S3", "SONG_DATA"),
)


def get_config():
    """
    Get configuration variables from the data warehouse config file.
    This function assumes that there is a file named dwh.cfg in the current
    working directory.
    The config file is expected to have the following
    "sections" constaining the listed "keys".
    Section:    [AWS]
        Keys:       KEY
                    SECRET
                    REGION
    Section:    [DWH]
        Keys:       CLUSTER_TYPE
                    NUM_NODES
                    NODE_TYPE
                    IAM_ROLE_NAME
                    CLUSTER_IDENTIFIER
                    DB
                    DB_USER
                    DB_PASSWORD
                    PORT
    Section:    [S3]
        Keys:       LOG_DATA
                    LOG_JSONPATH
                    SONG_DATA
    """
    return _dwh_cfg
