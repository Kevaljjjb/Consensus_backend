import os
import sys

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from api.listing_filters import reset_numeric_columns_cache


@pytest.fixture(autouse=True)
def reset_listing_filter_cache():
    reset_numeric_columns_cache()
    yield
    reset_numeric_columns_cache()
