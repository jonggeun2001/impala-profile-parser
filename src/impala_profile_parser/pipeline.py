"""Local input traversal and sequential conversion orchestration."""

import json
from contextlib import closing
import logging
import os
from pathlib import Path
import stat
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .decoder import ProfileError, decode_line
from .metrics import extract_record
from .output import OutputTransaction
from .schema import SCHEMA

LOG = logging.getLogger(__name__)
DEFAULT_LIMIT = 64 * 1024 * 1024


class InvocationError(ValueError):
    """Invalid CLI options or paths; its message is a safe reason code."""


class ConversionError(Exception):
    """Input processing failed; no output may be published."""


def validate_options(
    input_path, output_dir, timezone, batch_size, max_line_bytes, max_decoded_bytes
):
    if min(batch_size, max_line_bytes, max_decoded_bytes) <= 0:
        raise InvocationError("limits_must_be_positive")
    try:
        ZoneInfo(timezone)
    except (ValueError, ZoneInfoNotFoundError):
        raise InvocationError("invalid_timezone") from None
    original = Path(input_path).expanduser()
    if original.is_symlink():
        raise InvocationError("symlink_input")
    try:
        source = original.resolve(strict=True)
        destination = Path(output_dir).expanduser().resolve()
    except (OSError, RuntimeError):
        raise InvocationError("invalid_path") from None
    if not (source.is_file() or source.is_dir()):
        raise InvocationError("input_not_file_or_directory")
    if destination.exists() and not destination.is_dir():
        raise InvocationError("output_not_directory")
    if source.is_relative_to(destination) or destination.is_relative_to(source):
        raise InvocationError("input_output_overlap")
    return source, destination


def _input_failure(display, reason):
    LOG.error("source=%s reason=%s", json.dumps(display), reason)
    raise ConversionError(reason) from None


def _walk(directory_fd, prefix=""):
    # Anchor all descendants to open directory descriptors. O_NOFOLLOW on the
    # leaf alone does not stop an ancestor directory being replaced by a link.
    try:
        with os.scandir(directory_fd) as scan:
            entries = []
            for entry in scan:
                if entry.name.startswith(".") or entry.is_symlink():
                    continue
                is_directory = entry.is_dir(follow_symlinks=False)
                if is_directory or entry.is_file(follow_symlinks=False):
                    entries.append((entry.name, is_directory))
        # Including '/' in directory sort keys gives full relative-path order
        # without keeping the complete recursive file list in memory.
        entries.sort(key=lambda item: item[0] + ("/" if item[1] else ""))
    except OSError:
        _input_failure(prefix or ".", "input_scan_failed")
    for name, is_directory in entries:
        display = prefix + name
        if is_directory:
            try:
                child_fd = os.open(
                    name,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                    dir_fd=directory_fd,
                )
            except OSError:
                _input_failure(display, "input_directory_open_failed")
            try:
                yield from _walk(child_fd, display + "/")
            finally:
                os.close(child_fd)
        else:
            yield display, directory_fd, name


def _files(source, is_directory):
    if not is_directory:
        yield source.name, None, source
        return
    try:
        root_fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError:
        _input_failure(".", "input_directory_open_failed")
    try:
        yield from _walk(root_fd)
    finally:
        os.close(root_fd)


def _signature(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns


def convert(
    input_path,
    output_dir,
    *,
    timezone="UTC",
    batch_size=1024,
    max_line_bytes=DEFAULT_LIMIT,
    max_decoded_bytes=DEFAULT_LIMIT,
):
    source, destination = validate_options(
        input_path,
        output_dir,
        timezone,
        batch_size,
        max_line_bytes,
        max_decoded_bytes,
    )
    is_directory = source.is_dir()
    files = _files(source, is_directory)
    metadata = dict(SCHEMA.metadata or {})
    metadata[b"impala.source.timezone"] = timezone.encode("utf-8")
    with (
        closing(files),
        OutputTransaction(
            destination, SCHEMA.with_metadata(metadata), batch_size=batch_size
        ) as output,
    ):
        for display, parent_fd, name in files:
            try:
                fd = os.open(
                    name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent_fd
                )
            except OSError:
                _input_failure(display, "input_open_failed")
            try:
                with os.fdopen(fd, "rb") as stream:
                    _read_file(
                        stream,
                        display,
                        parent_fd,
                        name,
                        output,
                        timezone,
                        max_line_bytes,
                        max_decoded_bytes,
                    )
            except OSError:
                _input_failure(display, "input_read_failed")
        return output.commit()


def _read_file(
    stream,
    display,
    parent_fd,
    name,
    output,
    timezone,
    max_line_bytes,
    max_decoded_bytes,
):
    before = os.fstat(stream.fileno())
    if not stat.S_ISREG(before.st_mode):
        _input_failure(display, "input_not_regular_file")
    line_number = 0
    while True:
        line = stream.readline(max_line_bytes + 1)
        if not line:
            break
        line_number += 1
        try:
            if len(line) > max_line_bytes:
                raise ConversionError("line_too_large")
            if not line.strip():
                continue
            profile = decode_line(line, max_decoded_bytes=max_decoded_bytes)
            record = extract_record(profile, display, line_number, timezone)
            del profile
            output.add(record)
            if record["warningCount"]:
                LOG.warning(
                    "source=%s line=%d reason=optional_field_invalid count=%d",
                    json.dumps(display),
                    line_number,
                    record["warningCount"],
                )
        except (ProfileError, ConversionError) as error:
            LOG.error(
                "source=%s line=%d reason=%s", json.dumps(display), line_number, error
            )
            raise ConversionError("input_record_failed") from None
    if _signature(before) != _signature(os.fstat(stream.fileno())):
        _input_failure(display, "input_changed_during_read")
    if _signature(before) != _signature(
        os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    ):
        _input_failure(display, "input_replaced_during_read")
