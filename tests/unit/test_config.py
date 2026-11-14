from gamepulse.config import get_settings


def test_defaults(monkeypatch):
    keys = [
        "DATA_ROOT",
        "AWS_REGION",
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_TOPIC",
        "KAFKA_PARTITIONS",
        "SPARK_MASTER",
        "SPARK_CORES_MAX",
        "SPARK_SHUFFLE_PARTITIONS",
        "SPARK_EXECUTOR_MEMORY",
        "SPARK_DRIVER_MEMORY",
        "SPARK_DRIVER_HOST",
        "MAX_REJECT_RATE",
        "LOG_LEVEL",
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)

    settings = get_settings()
    assert settings.data_root == "/opt/gamepulse/data/lake"
    assert settings.aws_region == "us-east-1"
    assert settings.kafka_bootstrap_servers == "kafka:9092"
    assert settings.kafka_topic == "gamepulse.events.v1"
    assert settings.kafka_partitions == 6
    assert settings.spark_master == "local[2]"
    assert settings.spark_cores_max == 4
    assert settings.spark_shuffle_partitions == 8
    assert settings.spark_executor_memory == "1g"
    assert settings.spark_driver_memory == "1g"
    assert settings.spark_driver_host == "localhost"
    assert settings.max_reject_rate == 0.05
    assert settings.log_level == "INFO"


def test_override_and_types(monkeypatch):
    monkeypatch.setenv("DATA_ROOT", "/tmp/lake")
    monkeypatch.setenv("KAFKA_PARTITIONS", "3")
    monkeypatch.setenv("MAX_REJECT_RATE", "0.1")
    monkeypatch.setenv("SPARK_MASTER", "spark://spark-master:7077")
    settings = get_settings()
    assert settings.data_root == "/tmp/lake"
    assert settings.kafka_partitions == 3
    assert isinstance(settings.kafka_partitions, int)
    assert settings.max_reject_rate == 0.1
    assert isinstance(settings.max_reject_rate, float)
    assert settings.spark_master == "spark://spark-master:7077"


def test_not_cached(monkeypatch):
    monkeypatch.setenv("DATA_ROOT", "/a")
    assert get_settings().data_root == "/a"
    monkeypatch.setenv("DATA_ROOT", "/b")
    assert get_settings().data_root == "/b"
