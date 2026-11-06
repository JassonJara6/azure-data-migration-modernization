import sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[3]
for path in (ROOT,ROOT/'databricks/gold/src',ROOT/'databricks/silver/src'):sys.path.insert(0,str(path))
from local.lake import session

@pytest.fixture(scope='session')
def spark(tmp_path_factory):
    root=tmp_path_factory.mktemp('gold-spark');spark=session(root)
    yield spark
    spark.stop()
