"""
Shared PySpark+Iceberg session factory for delta-forge UniForm readback.

Usage in verify scripts:
    from verify_lib.spark_session_iceberg import get_spark
    spark = get_spark()
    df = spark.read.format("iceberg").load(table_path)

The session is configured with the Apache Iceberg Spark connector. delta-forge
writes Delta tables with `delta.universalFormat.enabledFormats = 'iceberg'`,
producing Iceberg metadata under `<table>/metadata/`. The Iceberg Spark reader
loads those tables by path (no catalog registration needed).

Spark 4.0 + Iceberg 1.10.x are the canonical versions for this skill. Iceberg
1.10.x is compatible with the Spark 4.0 line; iceberg-spark-runtime-4.0 is the
matching runtime artifact.
"""

import os
import subprocess
import sys


SPARK_VERSION = "4.1.1"
ICEBERG_VERSION = "1.10.1"
ICEBERG_SPARK_PACKAGE = (
    f"org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:{ICEBERG_VERSION}"
)


for _import_name, _pip_pkg in (
    ("pyspark", f"pyspark=={SPARK_VERSION}"),
):
    try:
        __import__(_import_name)
    except ImportError:
        print(f"  Installing missing dependency: {_pip_pkg}")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", _pip_pkg, "-q"],
            stdout=subprocess.DEVNULL,
        )


if not os.environ.get("JAVA_HOME"):
    _JDK_CANDIDATES = [
        "/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home",
        "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
        "/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home",
        "/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home",
        os.path.expanduser("~/local/jdk"),
        os.path.expanduser("~/.jdks/temurin-17"),
        os.path.expanduser("~/.jdks/temurin-21"),
        "/usr/lib/jvm/java-21-openjdk-amd64",
        "/usr/lib/jvm/java-17-openjdk-amd64",
    ]
    for _jh in _JDK_CANDIDATES:
        if os.path.isfile(os.path.join(_jh, "bin", "java")):
            os.environ["JAVA_HOME"] = _jh
            break
    if not os.environ.get("JAVA_HOME"):
        print("Error: JAVA_HOME is not set and no JDK 17+ found.")
        print("Spark 4.0 requires JDK 17+.")
        sys.exit(1)


_session = None


def get_spark():
    """Return a cached SparkSession wired with the Iceberg Spark connector.

    The Iceberg runtime jar is pulled from Maven Central on first use and cached
    under ~/.ivy2/ for subsequent runs. Tables are loaded by path with
    `spark.read.format("iceberg").load(<path>)`, so no catalog needs to be
    registered.
    """
    global _session
    if _session is not None:
        try:
            _session.sparkContext._jsc.sc().isStopped()
            return _session
        except Exception:
            _session = None

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
            .appName("delta-forge-iceberg-readback")
            .master("local[*]")
            .config("spark.jars.packages", ICEBERG_SPARK_PACKAGE)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog",
            )
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.driver.memory", "4g")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.log.level", "WARN")
            .config("spark.sql.session.timeZone", "UTC")
    )
    _session = builder.getOrCreate()
    return _session


def stop_spark():
    """Explicitly stop the cached SparkSession."""
    global _session
    if _session is not None:
        _session.stop()
        _session = None
