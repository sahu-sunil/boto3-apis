"""Module handels various AWS service and clients."""
import logging

import boto3
from boto3.session import Session
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


class AWSBaseSession:
    def __init__(
        self,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        aws_session_token=None,
    ):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self._session = None

    @property
    def session(self):
        if not self._session:
            self._session = Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
            )
        return self._session

    def assume_role(self, role_arn: str, role_session: str, aws_secrets: dict):
        sts_client = boto3.client(
            "sts",
            **aws_secrets,
        )
        try:
            resp = sts_client.assume_role(
                RoleArn=role_arn, RoleSessionName=role_session, DurationSeconds=3600
            )
        except ClientError as exc:
            raise

        return resp["Credentials"]

    @staticmethod
    def parse_creds(aws_creds: dict) -> dict:
        return {
            "aws_access_key_id": aws_creds["AccessKeyId"],
            "aws_secret_access_key": aws_creds["SecretAccessKey"],
            "aws_session_token": aws_creds["SessionToken"],
        }

    def cfn_client(self, region):
        return self.session.client("cloudformation", region_name=region)

    def ec2_client(self, region="us-east-1"):
        return self.session.client("ec2", region_name=region)

    def dynamo_db_resource(self, region="eu-west-1"):
        return self.session.resource("dynamodb", region_name=region)

    def s3_resource(self, region):
        return self.session.resource("s3", region_name=region)

    def iam_client(self, region="us-east-1"):
        return self.session.client("iam", region_name=region)

    def ram_client(self, region):
        return self.session.client("ram", region_name=region)

    def route53_resolver_client(self, region):
        return self.session.client("route53resolver", region_name=region)

    def route53_client(self, region):
        return self.session.client("route53", region_name=region)

    def service_catalog_client(self, region):
        return self.session.client("servicecatalog", region_name=region)

    def get_all_regions(self):
        all_regions = []
        try:
            client = self.ec2_client()
            response = client.describe_regions()  # type: ignore
            all_regions = [region["RegionName"] for region in response["Regions"]]
        except Exception as exc:
            logger.exception(f"Error while getting all the aws regions: {exc}")
            raise

        return all_regions
