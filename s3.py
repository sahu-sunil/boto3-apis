import logging


logger = logging.getLogger(__name__)


class S3Bucket:
    def __init__(self, session_object, region_name):
        self.region_name = region_name
        self.session = session_object.client("s3", region_name=region_name)

    def get_bucket(self, bucket_name):
        bucket_response = {}
        try:
            bucket_response = self.session.head_bucket(Bucket=bucket_name)
        except self.session.exceptions.NoSuchBucket:
            
            logger.error(f"Bucket {bucket_name!r} does not exist")
        return bucket_response

    def is_bucket_exist(self, bucket_name):
        return bool(self.get_bucket(bucket_name))

    def get_bucket_lifecycle(self, bucket_name):
        bucket_lifecylce_response = []
        response = self.session.get_bucket_lifecycle(Bucket=bucket_name)
        bucket_lifecylce_response = response["Rules"]
        return bucket_lifecylce_response

    def get_bucket_policy(self, bucket_name):
        bucket_policy_response = ""
        response = self.session.get_bucket_policy(Bucket=bucket_name)
        bucket_policy_response = response["Policy"]
        return bucket_policy_response

    def get_bucket_tagging(self, bucket_name):
        bucket_tag_response = ""
        response = self.session.get_bucket_tagging(Bucket=bucket_name)
        bucket_tag_response = response["TagSet"]
        return bucket_tag_response

    def list_objects(self, bucket_name, prefix):
        kwargs = {"Bucket": bucket_name, "Prefix": prefix}
        resp = self.session.list_objects_v2(**kwargs)
        return resp.get("Contents", [])

    def download_objects(self, bucket_name, key, destination_pathname):
        response = self.session.download_file(bucket_name, key, destination_pathname)
        return response

    def upload_file(self, file_path, bucket_name, key):
        response = self.session.upload_file(
            file_path, bucket_name, key, ExtraArgs={"ServerSideEncryption": "aws:kms"}
        )
        return response

    def delete_objects(self, bucket_name, keys):
        response = self.session.delete_objects(Bucket=bucket_name, Delete=keys)
        return response

    def create_bucket(self, bucket_name):
        response = {}
        if self.region_name == "us-east-1":
            response = self.session.create_bucket(ACL="private", Bucket=bucket_name)
        else:
            response = self.session.create_bucket(
                ACL="private",
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": self.region_name},
            )
        return response

    def update_public_access_block(self, bucket_name):
        response = self.session.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": False,
                "IgnorePublicAcls": False,
                "BlockPublicPolicy": False,
                "RestrictPublicBuckets": False,
            },
        )
        return response

    def update_bucket_tags(self, bucket_name, tags):
        response = self.session.put_bucket_tagging(Bucket=bucket_name, Tagging={"TagSet": tags})
        return response

    def update_bucket_policy(self, bucket_name, policy):
        """
        Update the bucket Policy
        :param s3_client: S3 Client Connection
        :param bucket_name: Bucket to update the policy
        :return: True if policy updated on the bucket
        """
        response = self.session.put_bucket_policy(Bucket=bucket_name, Policy=policy)
        return response
