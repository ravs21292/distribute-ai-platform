"""S3-compatible object store (MinIO)."""

from __future__ import annotations

from io import BytesIO

import boto3
from botocore.client import Config

from dap_common.config import Settings


class ObjectStore:
    def __init__(self, settings: Settings) -> None:
        self.bucket = settings.s3_bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.s3_region,
            config=Config(signature_version="s3v4"),
        )

    def ensure_bucket(self) -> None:
        try:
            self._client.head_bucket(Bucket=self.bucket)
        except Exception:
            self._client.create_bucket(Bucket=self.bucket)

    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        self._client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)

    def get_bytes(self, key: str) -> bytes:
        obj = self._client.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()

    def put_text(self, key: str, text: str) -> None:
        self.put_bytes(key, text.encode("utf-8"), content_type="text/plain; charset=utf-8")

    def get_text(self, key: str) -> str:
        return self.get_bytes(key).decode("utf-8", errors="replace")

    def upload_fileobj(self, key: str, fileobj: BytesIO, content_type: str = "application/octet-stream") -> None:
        self._client.upload_fileobj(fileobj, self.bucket, key, ExtraArgs={"ContentType": content_type})
