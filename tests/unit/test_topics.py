from gamepulse.ingestion.topics import dlq_topic, events_topic


def test_events_topic_default(monkeypatch):
    monkeypatch.delenv("KAFKA_TOPIC", raising=False)
    assert events_topic() == "gamepulse.events.v1"


def test_dlq_topic_suffix(monkeypatch):
    monkeypatch.setenv("KAFKA_TOPIC", "gamepulse.events.v1")
    assert dlq_topic() == "gamepulse.events.v1.dlq"
