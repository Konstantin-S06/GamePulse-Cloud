"""Spark session builders."""

from __future__ import annotations

from pyspark.sql import SparkSession

from gamepulse.config import get_settings


def build_spark(app_name: str) -> SparkSession:
    """Build a Spark session from environment settings."""
    settings = get_settings()
    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"s3.{settings.aws_region}.amazonaws.com",
        )
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    )
    if settings.spark_master.startswith("spark://"):
        builder = (
            builder.config("spark.executor.memory", settings.spark_executor_memory)
            .config("spark.executor.cores", "2")
            .config("spark.cores.max", str(settings.spark_cores_max))
            .config("spark.driver.host", settings.spark_driver_host)
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.driver.port", "40000")
            .config("spark.blockManager.port", "40001")
        )
    return builder.getOrCreate()


def build_local_spark() -> SparkSession:
    """Local Spark for unit and spark tests."""
    return (
        SparkSession.builder.master("local[2]")
        .appName("gamepulse-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
