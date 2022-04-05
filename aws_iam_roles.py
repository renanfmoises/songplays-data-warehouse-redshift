import json
import boto3
from botocore.exceptions import ClientError

from aws_params import get_params

def get_iam_client(region_name, aws_access_key_id, aws_secret_access_key):
    """This function returns an IAM client.
    """
    iam_client = boto3.client(
        "iam",
        region_name             = region_name,
        aws_access_key_id       = aws_access_key_id,
        aws_secret_access_key   = aws_secret_access_key,
    )
    return iam_client


def attach_role_policy(iam_client, role_name):
    """This function attaches the IAM policy named "dwhRolePolicy" to the IAM role
       named role_name.
    """
    try:
        print(f"Attaching the IAM policy named dwhRolePolicy to the IAM role {role_name}...")
        iam_client.attach_role_policy(
            RoleName = role_name,
            PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
    except ClientError as e:
        print(e)
        if e.response['Error']['Code'] == 'InvalidInput':
            print(f"The IAM policy named dwhRolePolicy is already attached to the IAM role {role_name}.")
        else:
            raise
    else:
        print(f"The IAM policy named dwhRolePolicy has been attached to the IAM role {role_name}.")


def create_role(iam_client, role_name, role_policy_document):
    """ This function creates an IAM role.
        The role is created with the name role_name and the policy document
        role_policy_document.
    """
    try:
        print(f"Creating a new IAM role named {role_name}...")
        iam_client.create_role(
            Path      = "/",
            RoleName  = role_name,
            Description="Main Redshift role for the data warehouse",
            AssumeRolePolicyDocument = json.dumps(role_policy_document)
        )
    except ClientError as e:
        print(e)
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print(f"The IAM role {role_name} already exists.")
        else:
            raise
    else:
        print(f"The IAM role {role_name} has been created.")


def check_role(iam_client, role_name):
    """This function checks if the IAM role named role_name exists.
    """
    try:
        print(f"Checking if the IAM role {role_name} exists...")
        iam_client.get_role(RoleName=role_name)
    except ClientError as e:
        print(e)
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print(f"The IAM role {role_name} does not exist.")
            return False
        else:
            raise
    else:
        print(f"The IAM role {role_name} exists.")
        return True


def detach_role_policy(iam_client, role_name):
    """This function detaches the IAM policy named "dwhRolePolicy" from the IAM role
       named role_name.
    """
    try:
        print(f"Detaching the IAM policy named dwhRolePolicy from the IAM role {role_name}...")
        iam_client.detach_role_policy(
            RoleName = role_name,
            PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
    except ClientError as e:
        print(e)
        if e.response['Error']['Code'] == 'InvalidInput':
            print(f"The IAM policy named dwhRolePolicy is not attached to the IAM role {role_name}.")
        else:
            raise
    else:
        print(f"The IAM policy named dwhRolePolicy has been detached from the IAM role {role_name}.")


def delete_role(iam_client, role_name):
    """This function deletes the IAM role named role_name.
    """
    try:
        print(f"Deleting the IAM role named {role_name}...")
        iam_client.delete_role(RoleName=role_name)
    except ClientError as e:
        print(e)
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print(f"The IAM role {role_name} does not exist.")
        else:
            raise
    else:
        print(f"The IAM role {role_name} has been deleted.")


def get_role_arn(iam_client, role_name):
    """This function returns the ARN of the IAM role named role_name.
    """
    try:
        print(f"Getting the ARN of the IAM role named {role_name}...")
        role_arn = iam_client.get_role(RoleName=role_name)['Role']['Arn']
    except ClientError as e:
        print(e)
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print(f"The IAM role {role_name} does not exist.")
        else:
            raise
    else:
        print(f"The ARN of the IAM role named {role_name} is {role_arn}.")
        return role_arn


def main():
    role_policy_document = {
        "Statement": [
            {
                "Action": "sts:AssumeRole",
                "Effect": "Allow",
                "Principal": {"Service": "redshift.amazonaws.com"},
                }
            ],
        "Version": "2012-10-17",
        }

    awsParams = get_params()

    iam_client = get_iam_client(
        region_name = awsParams.REGION,
        aws_access_key_id = awsParams.KEY,
        aws_secret_access_key = awsParams.SECRET
        )

    create_role(
        iam_client = iam_client,
        role_name = awsParams.IAM_ROLE_NAME,
        role_policy_document = role_policy_document
        )

    attach_role_policy(iam_client = iam_client, role_name = awsParams.IAM_ROLE_NAME)

    check_role(iam_client = iam_client, role_name = awsParams.IAM_ROLE_NAME)


if __name__ == "__main__":
    main()
