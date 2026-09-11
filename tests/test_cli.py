import base64
from pathlib import Path
import subprocess
import sys
import zlib

import pyarrow.parquet as pq
from thrift.protocol.TCompactProtocol import TCompactProtocolFactory
from thrift.TSerialization import serialize

from impala_profile_parser.vendor.RuntimeProfile.ttypes import TRuntimeProfileNode, TRuntimeProfileTree, TCounter
from impala_profile_parser.vendor.Metrics.ttypes import TUnit


def profile_line(query_id="1:2"):
    def node(name, children=0, info=None, counters=None):
        return TRuntimeProfileNode(
            name=name, num_children=children, counters=counters or [], metadata=-1,
            indent=True, info_strings=info or {}, info_strings_display_order=[],
            child_counters_map={},
        )
    tree = TRuntimeProfileTree(nodes=[
        node("Query (id=" + query_id + ")", 2),
        node("Summary", info={
            "Impala Version": "impalad version 3.4.0.7.1.7.0-551 RELEASE",
            "User": "analyst", "Sql Statement": "select\n  1 -- keep whitespace",
            "Start Time": "2026-09-11 10:00:00.123456789",
            "End Time": "2026-09-11 10:00:01.456789000",
            "Query Type": "QUERY", "Query State": "FINISHED", "Query Status": "OK",
        }),
        node("ImpalaServer", counters=[TCounter(name="NumRowsFetched", unit=TUnit.UNIT, value=0)]),
    ])
    payload = serialize(tree, protocol_factory=TCompactProtocolFactory())
    return b"1789088400000 " + query_id.encode() + b" " + base64.b64encode(zlib.compress(payload)) + b"\n"


def cli(*args):
    return subprocess.run([sys.executable, "-m", "impala_profile_parser", *map(str, args)], capture_output=True, text=True)


def test_cli_converts_real_encoded_records_to_parquet(tmp_path):
    source = tmp_path / "input.log"
    source.write_bytes(profile_line() + b"\n" + profile_line())
    result = cli("--input", source, "--output", tmp_path / "out", "--timezone", "Asia/Seoul", "--batch-size", 1)
    assert result.returncode == 0, result.stderr
    table = pq.read_table(tmp_path / "out/result.parquet")
    assert table.num_rows == 2
    assert table["queryId"].to_pylist() == ["0000000000000001:0000000000000002"] * 2
    assert table["query"].to_pylist() == ["select\n  1 -- keep whitespace"] * 2
    assert table["resultRows"].to_pylist() == [0, 0]
    assert table["sourceLine"].to_pylist() == [1, 3]
    assert table["startTime"].to_pylist()[0].hour == 1
    assert "select" not in result.stdout + result.stderr


def test_corrupt_later_record_preserves_previous_output(tmp_path):
    source = tmp_path / "input.log"
    source.write_bytes(profile_line())
    out = tmp_path / "out"
    assert cli("--input", source, "--output", out).returncode == 0
    previous = (out / "result.parquet").read_bytes()
    source.write_bytes(profile_line() + b"1789088400000 1:2 SECRET_SQL\n")
    result = cli("--input", source, "--output", out, "--batch-size", 1)
    assert result.returncode == 1
    assert "SECRET_SQL" not in result.stderr
    assert (out / "result.parquet").read_bytes() == previous


def test_empty_directory_creates_zero_rows(tmp_path):
    source = tmp_path / "input"
    source.mkdir()
    result = cli("--input", source, "--output", tmp_path / "out")
    assert result.returncode == 0, result.stderr
    assert pq.read_metadata(tmp_path / "out/result.parquet").num_rows == 0


def test_deterministic_recursion_ignores_hidden_and_symlinks(tmp_path):
    source = tmp_path / "input"
    (source / "nested").mkdir(parents=True)
    (source / "z.log").write_bytes(profile_line("1:3"))
    (source / "nested/a.log").write_bytes(profile_line("1:2"))
    (source / ".hidden").write_text("bad data")
    (source / "link.log").symlink_to(source / "z.log")
    (source / "loop").symlink_to(source, target_is_directory=True)
    result = cli("--input", source, "--output", tmp_path / "out")
    assert result.returncode == 0, result.stderr
    assert pq.read_table(tmp_path / "out/result.parquet")["sourceFile"].to_pylist() == ["nested/a.log", "z.log"]


def test_overlap_missing_path_and_invalid_options_exit_two(tmp_path):
    source = tmp_path / "input"
    source.mkdir()
    for args in [
        ("--input", source, "--output", source / "out"),
        ("--input", tmp_path / "missing", "--output", tmp_path / "out"),
        ("--input", source, "--output", tmp_path / "out", "--batch-size", 0),
        ("--input", source, "--output", tmp_path / "out", "--timezone", "Invalid/Zone"),
    ]:
        result = cli(*args)
        assert result.returncode == 2, result.stderr


def test_line_limit_fails_before_decoding(tmp_path):
    source = tmp_path / "input.log"
    source.write_bytes(profile_line())
    result = cli("--input", source, "--output", tmp_path / "out", "--max-line-bytes", 10)
    assert result.returncode == 1
    assert "line_too_large" in result.stderr
    assert not (tmp_path / "out/result.parquet").exists()


def test_lock_is_held_across_processes(tmp_path):
    import fcntl

    source = tmp_path / "input.log"
    source.write_bytes(profile_line())
    out = tmp_path / "out"
    out.mkdir()
    with (out / ".impala-profile-parser.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = cli("--input", source, "--output", out)
    assert result.returncode == 3, result.stderr


def test_explicit_symlink_input_is_rejected(tmp_path):
    actual = tmp_path / "actual"
    actual.write_bytes(profile_line())
    source = tmp_path / "link"
    source.symlink_to(actual)
    assert cli("--input", source, "--output", tmp_path / "out").returncode == 2


def test_help_version_and_installed_console_script():
    assert cli("--help").returncode == 0
    assert "0.0.1" in cli("--version").stdout
    result = subprocess.run([str(Path(sys.executable).parent / "impala-profile-parser"), "--help"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_independent_impala34_wire_fixture(tmp_path):
    source = Path(__file__).parent / "fixtures/impala-3.4-synthetic.log"
    result = cli("--input", source, "--output", tmp_path / "out", "--timezone", "UTC")
    assert result.returncode == 0, result.stderr
    row = pq.read_table(tmp_path / "out/result.parquet").to_pylist()[0]
    assert row["queryId"] == "0000000000000001:0000000000000002"
    assert row["profileVersion"] == 1
    assert row["user"] == "fixture_user"
    assert row["query"] == "select\n  42 -- synthetic fixture"
    assert row["durationMilliseconds"] == 1250
    assert row["resultRows"] == 1
    assert row["resultRowsKind"] == "CLIENT_FETCHED"


def test_input_changed_during_read_is_not_published(tmp_path, monkeypatch):
    import os
    from impala_profile_parser import pipeline

    source = tmp_path / "input.log"
    source.write_bytes(profile_line())
    original_decode = pipeline.decode_line

    def changing_input(line, **kwargs):
        record = original_decode(line, **kwargs)
        current = source.stat()
        os.utime(source, ns=(current.st_atime_ns, current.st_mtime_ns + 1_000_000))
        return record

    monkeypatch.setattr(pipeline, "decode_line", changing_input)
    import pytest
    with pytest.raises(pipeline.ConversionError, match="input_changed_during_read"):
        pipeline.convert(source, tmp_path / "out")
    assert not (tmp_path / "out/result.parquet").exists()


def test_relative_paths_are_globally_lexicographic(tmp_path):
    source = tmp_path / "input"
    (source / "a").mkdir(parents=True)
    (source / "a/item.log").write_bytes(profile_line())
    (source / "a-foo.log").write_bytes(profile_line())
    result = cli("--input", source, "--output", tmp_path / "out")
    assert result.returncode == 0, result.stderr
    assert pq.read_table(tmp_path / "out/result.parquet")["sourceFile"].to_pylist() == ["a-foo.log", "a/item.log"]


def test_directory_swap_to_symlink_cannot_read_outside_input(tmp_path, monkeypatch):
    import os
    import pytest
    from impala_profile_parser import pipeline

    source = tmp_path / "input"
    nested = source / "nested"
    nested.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "outside.log").write_bytes(profile_line("3:4"))
    real_scandir = os.scandir
    swapped = False

    class Entry:
        def __init__(self, entry):
            self.entry = entry
            self.name = entry.name
            self.path = entry.path

        def is_symlink(self):
            return self.entry.is_symlink()

        def is_file(self, **kwargs):
            return self.entry.is_file(**kwargs)

        def is_dir(self, **kwargs):
            nonlocal swapped
            result = self.entry.is_dir(**kwargs)
            if self.name == "nested" and not swapped:
                swapped = True
                nested.rename(source / "old")
                nested.symlink_to(outside, target_is_directory=True)
            return result

    class Scan:
        def __init__(self, path):
            self.scan = real_scandir(path)

        def __enter__(self):
            return (Entry(entry) for entry in self.scan)

        def __exit__(self, *args):
            self.scan.close()

    monkeypatch.setattr(pipeline.os, "scandir", Scan)
    with pytest.raises(pipeline.ConversionError):
        pipeline.convert(source, tmp_path / "out")
    assert swapped
    assert not (tmp_path / "out/result.parquet").exists()
