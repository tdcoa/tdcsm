"global setup including fixtures"
from pathlib import Path
import pytest


@pytest.fixture(scope="session")
def testdir(request) -> Path:
	return Path(request.config.rootdir) / "tests"
