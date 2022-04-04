import boto3


def get_aws_clients(cfg):
    """
    Get all of the AWS clients that are required for this project.
    :param cfg: a config object for this project
    """
    return {
        "ec2": get_ec2_client(cfg),
        "iam": get_iam_client(cfg),
        "rs": get_redshift_client,
    }


def get_ec2_client(cfg):
    """
    Get an EC2 client.
    :param cfg: a config object for this project
    """
    return boto3.resource(
        "ec2",
        aws_access_key_id=cfg.ACCESS_KEY,
        aws_secret_access_key=cfg.SECRET_KEY,
        region_name=cfg.REGION,
    )


def get_iam_client(cfg):
    """
    Get an IAM client.
    :param cfg: a config object for this project
    """
    return boto3.client(
        "iam",
        aws_access_key_id=cfg.ACCESS_KEY,
        aws_secret_access_key=cfg.SECRET_KEY,
        region_name=cfg.REGION,
    )


def get_redshift_client(cfg):
    """
    Get a Redshift client.
    :param cfg: a config object for this project
    """
    return boto3.client(
        "redshift",
        aws_access_key_id=cfg.ACCESS_KEY,
        aws_secret_access_key=cfg.SECRET_KEY,
        region_name=cfg.REGION,
    )
