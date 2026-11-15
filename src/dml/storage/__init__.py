"""Object storage: MinIO/S3 in Compose, local filesystem for tests."""

from __future__ import annotations

from pathlib import Path
from typing import BinaryIO, Protocol

from dml.config import Settings


class ObjectStore(Protocol):
    def ensure_ready(self) -> None: ...

    def put_bytes(self, key: str, data: bytes) -> None: ...

    def put_fileobj(self, key: str, fileobj: BinaryIO) -> None: ...

    def get_bytes(self, key: str) -> bytes: ...

    def open_read(self, key: str) -> BinaryIO: ...

    def exists(self, key: str) -> bool: ...


class LocalObjectStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def ensure_ready(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        path = self.root.joinpath(*key.split("/"))
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def put_bytes(self, key: str, data: bytes) -> None:
        self._path(key).write_bytes(data)

    def put_fileobj(self, key: str, fileobj: BinaryIO) -> None:
        with self._path(key).open("wb") as out:
            while True:
                chunk = fileobj.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)

    def get_bytes(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def open_read(self, key: str) -> BinaryIO:
        return self._path(key).open("rb")

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()


class S3ObjectStore:
    def __init__(self, settings: Settings) -> None:
        import boto3
        from botocore.client import Config

        self.bucket = settings.s3_bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.s3_region,
            config=Config(s3={"addressing_style": "path"}),
        )

    def ensure_ready(self) -> None:
        try:
            self._client.head_bucket(Bucket=self.bucket)
        except Exception:
            self._client.create_bucket(Bucket=self.bucket)

    def put_bytes(self, key: str, data: bytes) -> None:
        self._client.put_object(Bucket=self.bucket, Key=key, Body=data)

    def put_fileobj(self, key: str, fileobj: BinaryIO) -> None:
        self._client.upload_fileobj(fileobj, self.bucket, key)

    def get_bytes(self, key: str) -> bytes:
        response = self._client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read()

    def open_read(self, key: str) -> BinaryIO:
        response = self._client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"]

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            return False


def build_object_store(settings: Settings) -> LocalObjectStore | S3ObjectStore:
    if settings.object_store == "local":
        return LocalObjectStore(settings.local_object_root)
    return S3ObjectStore(settings)
