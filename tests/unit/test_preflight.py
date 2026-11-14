from gamepulse.ops import preflight


def test_preflight_ok_local(monkeypatch):
    monkeypatch.setenv("DATA_ROOT", "/tmp/lake")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "gamepulse.events.v1")
    monkeypatch.setenv("SPARK_MASTER", "local[2]")
    assert preflight.main() == 0


def test_preflight_missing_kafka(monkeypatch):
    monkeypatch.setenv("DATA_ROOT", "/tmp/lake")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "")
    monkeypatch.setenv("SPARK_MASTER", "local[2]")
    assert preflight.main() == 1
