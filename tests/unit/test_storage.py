import pyarrow as pa
import pyarrow.fs as pafs

from gamepulse.common import storage


def test_join_and_is_s3():
    assert storage.is_s3("s3://bucket/key")
    assert not storage.is_s3("/tmp/x")
    assert storage.join_uri("/opt/lake/", "bronze", "events") == "/opt/lake/bronze/events"
    assert storage.join_uri("s3://b", "a", "c") == "s3://b/a/c"


def test_parquet_json_roundtrip(tmp_path):
    root = str(tmp_path)
    table = pa.table({"a": [1, 2], "b": ["x", "y"]})
    parquet_uri = storage.join_uri(root, "nested", "t.parquet")
    storage.write_parquet(table, parquet_uri)
    back = storage.read_parquet(parquet_uri)
    assert back.column_names == ["a", "b"]
    assert back.to_pydict() == {"a": [1, 2], "b": ["x", "y"]}

    json_uri = storage.join_uri(root, "nested", "m.json")
    storage.write_json({"k": 1}, json_uri)
    assert storage.read_json(json_uri) == {"k": 1}
    assert storage.exists(json_uri)


def test_list_delete_size(tmp_path):
    root = str(tmp_path)
    assert storage.list_files(storage.join_uri(root, "missing")) == []
    storage.write_json({"a": 1}, storage.join_uri(root, "p", "a.json"))
    storage.write_json({"b": 2}, storage.join_uri(root, "p", "sub", "b.json"))
    files = storage.list_files(storage.join_uri(root, "p"))
    assert len(files) == 2
    size = storage.dir_size_bytes(storage.join_uri(root, "p"))
    expected = sum(f.stat().st_size for f in (tmp_path / "p").rglob("*.json"))
    assert size == expected
    storage.delete_prefix(storage.join_uri(root, "p"))
    assert storage.list_files(storage.join_uri(root, "p")) == []
    storage.delete_prefix(storage.join_uri(root, "missing"))


def test_to_spark_and_open_fs_s3(monkeypatch):
    assert storage.to_spark_uri("s3://bucket/key") == "s3a://bucket/key"
    assert storage.to_spark_uri("/opt/lake") == "/opt/lake"
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    fs, path = storage.open_fs("s3://my-bucket/prefix")
    assert isinstance(fs, pafs.S3FileSystem)
    assert path == "my-bucket/prefix"
