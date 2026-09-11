"""Bounded Parquet writing with exclusive, atomic local publication."""

import fcntl
import logging
import os
from pathlib import Path
import stat
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

LOG = logging.getLogger(__name__)


class OutputLocked(Exception):
    """Another process owns the output directory."""


class OutputTransaction:
    def __init__(self, directory, schema, batch_size=1024, max_batch_bytes=16 * 1024 * 1024):
        if batch_size <= 0 or max_batch_bytes <= 0:
            raise ValueError("invalid_batch_limit")
        self.directory = Path(directory)
        self.schema = schema
        self.batch_size = batch_size
        self.max_batch_bytes = max_batch_bytes
        self.temporary_path = None
        self._lock_fd = None
        self._writer = None
        self._batch = []
        self._batch_bytes = 0
        self._rows = 0
        self._closed = False
        self._published = False

    def __enter__(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        try:
            self._lock_fd = os.open(
                self.directory / ".impala-profile-parser.lock",
                os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600,
            )
            if not stat.S_ISREG(os.fstat(self._lock_fd).st_mode):
                raise OSError("invalid_output_lock")
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise OutputLocked("output_locked") from None
            fd, temporary = tempfile.mkstemp(prefix=".result-", suffix=".tmp", dir=self.directory)
            os.close(fd)
            self.temporary_path = Path(temporary)
            self._writer = pq.ParquetWriter(
                self.temporary_path, self.schema, compression="snappy",
                version="1.0", coerce_timestamps="ms", allow_truncated_timestamps=False,
            )
            return self
        except BaseException:
            self._cleanup()
            raise

    def add(self, row):
        if self._writer is None or self._closed:
            raise ValueError("writer_not_open")
        # Required Arrow fields are a contract even before Parquet enforces them.
        for field in self.schema:
            if not field.nullable and row.get(field.name) is None:
                raise ValueError("missing_required_output_field")
        self._batch.append(row)
        self._batch_bytes += sum(
            len(value.encode("utf-8")) if isinstance(value, str) else 16
            for value in row.values()
        )
        if len(self._batch) >= self.batch_size or self._batch_bytes >= self.max_batch_bytes:
            self._flush()

    def _flush(self):
        if not self._batch:
            return
        table = pa.Table.from_pylist(self._batch, schema=self.schema)
        self._writer.write_table(table, row_group_size=len(self._batch))
        self._rows += len(self._batch)
        self._batch.clear()
        self._batch_bytes = 0

    def close_writer(self):
        if not self._closed:
            self._flush()
            self._writer.close()
            self._closed = True

    def commit(self):
        if self._published:
            raise ValueError("already_published")
        self.close_writer()
        with pq.ParquetFile(self.temporary_path) as result:
            if result.metadata.num_rows != self._rows:
                raise ValueError("output_row_count_mismatch")
            if not result.schema_arrow.equals(self.schema, check_metadata=True):
                raise ValueError("output_schema_mismatch")
        with self.temporary_path.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(self.temporary_path, self.directory / "result.parquet")
        self._published = True
        self.temporary_path = None
        return self._rows

    def _cleanup(self):
        if self._writer is not None and not self._closed:
            try:
                self._writer.close()
            except Exception:
                LOG.warning("stage=cleanup reason=writer_close_failed")
            self._closed = True
        self._batch.clear()
        if self.temporary_path is not None:
            try:
                self.temporary_path.unlink(missing_ok=True)
            except OSError:
                LOG.warning("stage=cleanup reason=temporary_cleanup_failed")
        if self._lock_fd is not None:
            try:
                os.close(self._lock_fd)
            except OSError:
                LOG.warning("stage=cleanup reason=lock_close_failed")
            self._lock_fd = None

    def __exit__(self, exc_type, exc_value, traceback):
        self._cleanup()
