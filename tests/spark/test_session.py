import pytest

from gamepulse.processing.session import build_local_spark

pytestmark = pytest.mark.spark


def test_build_local_spark():
    spark = build_local_spark()
    try:
        assert spark.sparkContext.master == "local[2]"
        assert spark.conf.get("spark.sql.shuffle.partitions") == "2"
        assert spark.conf.get("spark.ui.enabled") == "false"
    finally:
        spark.stop()
