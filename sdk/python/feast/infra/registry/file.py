import uuid
from datetime import datetime
from pathlib import Path
import fsspec

from feast.infra.registry.registry_store import RegistryStore
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.repo_config import RegistryConfig
from feast.usage import log_exceptions_and_usage


class FileRegistryStore(RegistryStore):
    def __init__(self, registry_config: RegistryConfig, repo_path: Path):
        self.fs = fsspec.filesystem('file')
        self.path = registry_config.path

        if not Path(self.path).is_absolute():
            #TODO add tests for this
            self.path = repo_path.joinpath(Path(self.path)).as_uri()


    @log_exceptions_and_usage(registry="local")
    def get_registry_proto(self):
        registry_proto = RegistryProto()
        if self.fs.exists(self.path):
            with self.fs.open(self.path, mode="rb") as f:
                registry_proto.ParseFromString(f.read())
            return registry_proto
        raise FileNotFoundError(
            f'Registry not found at path "{self.path}". Have you run "feast apply"?'
        )

    @log_exceptions_and_usage(registry="local")
    def update_registry_proto(self, registry_proto: RegistryProto):
        self._write_registry(registry_proto)

    def teardown(self):
        try:
            self.fs.rm(self.path)
        except FileNotFoundError:
            # If the file deletion fails with FileNotFoundError, the file has already
            # been deleted.
            pass

    def _write_registry(self, registry_proto: RegistryProto):
        registry_proto.version_id = str(uuid.uuid4())
        registry_proto.last_updated.FromDatetime(datetime.utcnow())

        parent = self.fs._parent(self.path)
        if not self.fs.exists(parent):
            self.fs.mkdir(parent)

        with self.fs.open(self.path, mode="wb", buffering=0) as f:
            f.write(registry_proto.SerializeToString())
