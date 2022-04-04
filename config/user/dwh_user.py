import json


def attach_role_policy(iam_client, role_name):
    """
    Attaches an S3 read only policy to the specified IAM role.
    :param iam_client: A low-level client representing AWS Identity and Access
                       Management (IAM).
    :param role_name: The name (friendly name, not ARN) of the role to attach
                      the policy to.
    """
    iam_client.attach_role_policy(
        RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )


def check_role(iam_client, role_name):
    """
    A convenience function to check for the existence of a given role.
    :param iam_client: A low-level client representing AWS Identity and Access
                       Management (IAM).
    :param role_name: The name of the role to check.
    """
    try:
        iam_client.get_role(RoleName=role_name)
    except iam_client.exceptions.NoSuchEntityException:
        return False
    except Exception as e:
        print(e)
    return True


def create_role(iam_client, role_name):
    """
    Creates the Redshift role for the data warehouse and attaches an S3 read
    only policy to the role.
    :param iam_client: A low-level client representing AWS Identity and Access
                       Management (IAM).
    :param role_name: The name of the role to attach the policy to.
    """
    try:
        role = iam_client.create_role(
            Path="/",
            RoleName=role_name,
            Description="Main Redshift role for the data warehouse",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
        attach_role_policy(iam_client, role_name)
        return role
    except Exception as e:
        print(e)


def delete_role(iam_client, role_name):
    """
    Deletes the specified role. The role must not have any policies attached so
    this function calls detach_role_poicy.
    :param iam_client: A low-level client representing AWS Identity and Access
                       Management (IAM).
    :param role_name: The name of the role to delete.
    """
    try:
        detach_role_policy(iam_client, role_name)
        iam_client.delete_role(RoleName=role_name)

    except iam_client.exceptions.NoSuchEntityException:
        return None

    except Exception as e:
        print(e)


def detach_role_policy(iam_client, role_name):
    """
    Removes the S3 read only policy from the specified role.
    :param iam_client: A low-level client representing AWS Identity and Access
                       Management (IAM).
    :param role_name: The name of the role to detach the policy from.
    """
    iam_client.detach_role_policy(
        RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )


def get_role_arn(iam_client, role_name):
    """
    A convenience function to retrieve the Amazon Resource Name (ARN) of a role.
    :param iam_client: A low-level client representing AWS Identity and Access
                       Management (IAM).
    :param role_name: The role from which to get the ARN.
    """
    return iam_client.get_role(RoleName=role_name)["Role"]["Arn"]
