"""Configuration for pytest."""
import os
from typing import List

import _pytest
import pandas as pd
import pytest
from pandera.typing import DataFrame

from socceraction.atomic.spadl import AtomicSPADLSchema
from socceraction.spadl import SPADLSchema


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
        os.path.dirname(__file__), 'datasets', 'statsbomb', 'spadl-WorldCup-2018.h5'
    )
    store = pd.HDFStore(hdf_file, mode='r')
    yield store
    store.close()


@pytest.fixture(scope='session')
def spadl_actions() -> DataFrame[SPADLSchema]:
    json_file = os.path.join(os.path.dirname(__file__), 'datasets', 'spadl', 'spadl.json')
    return pd.read_json(json_file, orient='records')


@pytest.fixture(scope='session')
def atomic_spadl_actions() -> DataFrame[AtomicSPADLSchema]:
    json_file = os.path.join(os.path.dirname(__file__), 'datasets', 'spadl', 'atomic_spadl.json')
    return pd.read_json(json_file, orient='records')
