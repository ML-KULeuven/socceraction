"""Configuration for pytest."""
import os
from typing import List

import _pytest
import pandas as pd
import pytest


def pytest_addoption(parser: _pytest.config.argparsing.Parser) -> None:
    """Add command-line flags for pytest."""
    parser.addoption('--skip-slow', action='store_true', default=False, help='skip slow tests')


def pytest_configure(config: _pytest.config.Config) -> None:
    config.addinivalue_line('markers', 'slow: mark test as slow to run')


def pytest_collection_modifyitems(config: _pytest.config.Config, items: List[pytest.Item]) -> None:
    if config.getoption('--skip-slow'):
        skip_slow = pytest.mark.skip(reason='unset --skip-slow option to run')
        for item in items:
            if 'slow' in item.keywords:
                item.add_marker(skip_slow)


@pytest.fixture(scope='session')
def sb_worldcup_data() -> pd.HDFStore:
    hdf_file = os.path.join(
        os.path.dirname(__file__), 'data', 'statsbomb', 'spadl-WorldCup-2018.h5'
    )
    store = pd.HDFStore(hdf_file, mode='r')
    yield store
    store.close()
