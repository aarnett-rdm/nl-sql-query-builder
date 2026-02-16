import sys
from pathlib import Path
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TOOLS_DIR = PROJECT_ROOT / "tools"

sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(TOOLS_DIR))


@pytest.fixture(scope="session")
def metric_registry_path():
    # Your registry is in current/ not config/
    return PROJECT_ROOT / "current" / "metric_registry.json"
