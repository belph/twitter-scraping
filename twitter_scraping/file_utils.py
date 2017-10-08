import abc
import boto3
import os
import six
import time

from .auth import check_boto_credentials

@six.add_metaclass(abc.ABCMeta)
class ShardListener(object):
    def handle_shard(self, filename):
        """
        Called when a shard is finished being written to. 
        """
        pass

class S3FileMover(ShardListener):
    def __init__(self, bucket, base_dir, s3_root=None):
        assert bucket is not None, "Bucket name must not be None."
        check_boto_credentials()
        self._s3 = boto3.resource('s3')
        self._bucket_name = bucket
        self._base_dir = base_dir
        self._s3_root = s3_root or time.strftime("run_%Y-%m-%d_%H:%M:%S")
        if not any(b['Name'] == bucket for b in boto3.client('s3').list_buckets()['Buckets']):
            # Create the bucket if it doesn't exist
            self._s3.create_bucket(self._bucket_name)

    def move_file(self, filename):
        dest_filename = os.path.join(self._s3_root, os.path.relpath(filename, self._base_dir))
        bucket = self._s3.Bucket(self._bucket_name)
        bucket.upload_file(filename, dest_filename)
        obj = self._s3.Object(self._bucket_name, dest_filename)
        if os.stat(filename).st_size != obj.content_length:
            # TODO: Log something
            pass
        else:
            # Successfully uploaded. Delete old file
            os.remove(filename)

    def handle_shard(self, filename):
        self.move_file(filename)


class ShardedFileWriter(object):
    def __init__(self, directory, template):
        self._directory = directory
        self._template = template
        self._count = 0
        self._current_writer = None
        self._listener = None

    @property
    def current_filename(self):
        return os.path.join(self._directory, self._template.format(n=self._count))

    def offload_to_s3(self, bucket, s3_root=None):
        self._listener = S3FileMover(bucket, self._directory, s3_root=s3_root)
    
    def next_shard(self):
        if self._current_writer is not None:
            self._current_writer.close()
            if self._listener is not None:
                self._listener.handle_shard(self.current_filename)
        if not os.path.exists(self._directory):
            os.makedirs(self._directory)
        self._count += 1
        self._current_writer = open(self.current_filename, "w", encoding="utf-8")

    def close(self):
        if self._current_writer is not None:
            self._current_writer.close()
            if self._listener is not None:
                self._listener.handle_shard(self.current_filename)
        self._current_writer = None

    def __enter__(self):
        self.next_shard()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __getattr__(self, attr):
        if len(attr) == 0 or attr.startswith("_"):
            return object.__getattribute__(self, attr)
        else:
            return getattr(self._current_writer, attr)
