import os
import sys
import tempfile

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="smartgrid-test-"))


@pytest.fixture
def settings(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("NUM_HOUSEHOLDS", "40")
    from smartgrid.common import config
    config.get_settings.cache_clear()
    yield config.get_settings()
    config.get_settings.cache_clear()


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.master("local[1]").appName("smartgrid-tests")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    yield session
    session.stop()
    _ = pyspark
