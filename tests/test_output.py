import fcntl

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


SCHEMA = pa.schema([
    pa.field("queryId", pa.string(), nullable=False),
    pa.field("value", pa.int64()),
], metadata={b"impala.schema.version": b"1"})


def test_multiple_batches_and_null_zero_round_trip(tmp_path):
    from impala_profile_parser.output import OutputTransaction

    with OutputTransaction(tmp_path, SCHEMA, batch_size=2) as output:
        for value in [0, None, 7, 11, 13]:
            output.add({"queryId": "1:2", "value": value})
        assert output.commit() == 5
    table = pq.read_table(tmp_path / "result.parquet")
    assert table.column("value").to_pylist() == [0, None, 7, 11, 13]
    assert table.schema.equals(SCHEMA, check_metadata=True)


def test_empty_output_is_valid_parquet(tmp_path):
    from impala_profile_parser.output import OutputTransaction

    with OutputTransaction(tmp_path, SCHEMA) as output:
        assert output.commit() == 0
    assert pq.read_table(tmp_path / "result.parquet").num_rows == 0


def test_failed_conversion_preserves_existing_file(tmp_path):
    from impala_profile_parser.output import OutputTransaction

    target = tmp_path / "result.parquet"
    target.write_bytes(b"previous result")
    with pytest.raises(ValueError):
        with OutputTransaction(tmp_path, SCHEMA, batch_size=1) as output:
            output.add({"queryId": "1:2", "value": 1})
            raise ValueError("input failed")
    assert target.read_bytes() == b"previous result"
    assert not list(tmp_path.glob("*.tmp"))


def test_footer_corruption_prevents_publication(tmp_path):
    from impala_profile_parser.output import OutputTransaction

    target = tmp_path / "result.parquet"
    target.write_bytes(b"previous result")
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        with OutputTransaction(tmp_path, SCHEMA) as output:
            output.add({"queryId": "1:2", "value": 1})
            output.close_writer()
            output.temporary_path.write_bytes(b"broken parquet")
            output.commit()
    assert target.read_bytes() == b"previous result"


def test_existing_lock_rejects_writer(tmp_path):
    from impala_profile_parser.output import OutputLocked, OutputTransaction

    # A separately opened file description exercises the same OS lock as another process.
    with (tmp_path / ".impala-profile-parser.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(OutputLocked):
            with OutputTransaction(tmp_path, SCHEMA):
                pytest.fail("must not enter while locked")


def test_null_required_field_does_not_publish(tmp_path):
    from impala_profile_parser.output import OutputTransaction

    with pytest.raises((ValueError, pa.ArrowInvalid)):
        with OutputTransaction(tmp_path, SCHEMA) as output:
            output.add({"queryId": None, "value": 1})
            output.commit()
    assert not (tmp_path / "result.parquet").exists()


def test_size_threshold_flushes_without_waiting_for_row_limit(tmp_path):
    from impala_profile_parser.output import OutputTransaction

    with OutputTransaction(tmp_path, SCHEMA, batch_size=1000, max_batch_bytes=10) as output:
        output.add({"queryId": "1:2", "value": 7})
        output.add({"queryId": "1:2", "value": 8})
        output.commit()
    assert pq.read_metadata(tmp_path / "result.parquet").num_row_groups == 2


def test_lock_symlink_cannot_modify_unrelated_file(tmp_path):
    from impala_profile_parser.output import OutputTransaction

    target = tmp_path / "unrelated"
    target.write_text("keep")
    (tmp_path / ".impala-profile-parser.lock").symlink_to(target)
    with pytest.raises(OSError):
        with OutputTransaction(tmp_path, SCHEMA):
            pytest.fail("symlink lock should be rejected")
    assert target.read_text() == "keep"


def test_failed_atomic_replace_keeps_previous_result(tmp_path, monkeypatch):
    from impala_profile_parser.output import OutputTransaction

    target = tmp_path / "result.parquet"
    target.write_bytes(b"previous result")

    def fail_replace(*args):
        raise OSError("filesystem refused replacement")

    monkeypatch.setattr("impala_profile_parser.output.os.replace", fail_replace)
    with pytest.raises(OSError):
        with OutputTransaction(tmp_path, SCHEMA) as output:
            output.add({"queryId": "1:2", "value": 1})
            output.commit()
    assert target.read_bytes() == b"previous result"
    assert not list(tmp_path.glob("*.tmp"))
