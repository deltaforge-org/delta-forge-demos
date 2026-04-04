"""
Shared PySpark session factory and variable resolution for Delta demo verification.

Usage in verify scripts:
    from verify_lib.spark_session import get_spark, resolve_data_root

    data_root = resolve_data_root()
    spark = get_spark()

Variables are resolved in this order:
    1. CLI argument (positional `data_root`)
    2. Environment variable ``DEMO_DATA_PATH``
    3. Fail with a clear error message

This module is the single source of truth for Spark configuration
across all Delta demo verification scripts.

Spark 4.0 + Delta 4.0 (on-prem, no cloud dependencies).
"""

import argparse
import os
import subprocess
import sys


# ---------------------------------------------------------------------------
# Spark 4.0 / Delta 4.0 versions
# ---------------------------------------------------------------------------
SPARK_VERSION = "4.0.0"
DELTA_VERSION = "4.0.0"

# ---------------------------------------------------------------------------
# Auto-install missing pip dependencies
# ---------------------------------------------------------------------------
_REQUIRED = {
    "pyspark": f"pyspark=={SPARK_VERSION}",
    "delta": f"delta-spark=={DELTA_VERSION}",
}
for _import_name, _pip_pkg in _REQUIRED.items():
    try:
        __import__(_import_name)
    except ImportError:
        print(f"  Installing missing dependency: {_pip_pkg}")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", _pip_pkg, "-q"],
            stdout=subprocess.DEVNULL,
        )


# ---------------------------------------------------------------------------
# Auto-detect JAVA_HOME if not set (Spark 4.0 requires JDK 17+)
# ---------------------------------------------------------------------------
if not os.environ.get("JAVA_HOME"):
    _JDK_CANDIDATES = [
        os.path.expanduser("~/local/jdk"),
        os.path.expanduser("~/.jdks/temurin-17"),
        os.path.expanduser("~/.jdks/temurin-21"),
        "/usr/lib/jvm/java-17-openjdk-amd64",
        "/usr/lib/jvm/java-21-openjdk-amd64",
    ]
    for _jh in _JDK_CANDIDATES:
        if os.path.isfile(os.path.join(_jh, "bin", "java")):
            os.environ["JAVA_HOME"] = _jh
            break
    if not os.environ.get("JAVA_HOME"):
        # Last resort: try install-jdk
        try:
            import jdk
            _jh = jdk.install("17")
            os.environ["JAVA_HOME"] = _jh
        except Exception:
            print("Error: JAVA_HOME is not set and no JDK 17+ found.")
            print("Spark 4.0 requires JDK 17+.")
            print("Install a JDK, set JAVA_HOME, or: pip install install-jdk")
            sys.exit(1)



def get_spark():
    """Create a local SparkSession configured for Delta Lake.

    Uses delta-spark's configure_spark_with_delta_pip() to handle
    classpath setup automatically. Runs on-prem with Spark 4.0.
    """
    from pyspark.sql import SparkSession

    try:
        from delta import configure_spark_with_delta_pip
        builder = configure_spark_with_delta_pip(
            SparkSession.builder
                .appName("delta-verify")
                .master("local[*]")
                .config("spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.driver.memory", "2g")
                .config("spark.ui.showConsoleProgress", "false")
                .config("spark.log.level", "WARN")
        )
        return builder.getOrCreate()
    except ImportError:
        # Fallback: assume Delta JARs are on the classpath already.
        return SparkSession.builder \
            .appName("delta-verify") \
            .master("local[*]") \
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.driver.memory", "2g") \
            .config("spark.ui.showConsoleProgress", "false") \
            .config("spark.log.level", "WARN") \
            .getOrCreate()


def resolve_data_root(description="Verify Delta data for demo"):
    """Parse CLI args / env vars and return the absolute data_root path.

    Resolution order:
        1. Positional CLI arg ``data_root``
        2. Environment variable ``DEMO_DATA_PATH``
        3. Exit with error
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "data_root",
        nargs="?",
        default=os.environ.get("DEMO_DATA_PATH"),
        help="Root path containing table directories (or set DEMO_DATA_PATH env var)",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    if args.data_root is None:
        print("Error: data_root not provided and DEMO_DATA_PATH not set")
        sys.exit(1)

    return os.path.abspath(args.data_root), args.verbose
