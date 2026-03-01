import boto3
from settings import Minio


class BucketClient:
    def __init__(self):
        self.bucket_name = Minio.BUCKET_NAME
        self.connection = boto3.client(
            "s3",
            endpoint_url=Minio.HOST,
            aws_connect = Minio.CONNECTION
        )

    def get_files(self, prefix=""):
        response = self.connection.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=prefix
        )

        return [
            obj["Key"]
            for obj in response.get("Contents", [])
        ]