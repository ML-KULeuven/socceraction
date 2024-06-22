"""Configuration for pytest."""
import os
from collections.abc import Iterator

import pandas as pd
import pytest
from _pytest.config import Config
from pandera.typing import DataFrame

from socceraction.atomic.spadl import AtomicSPADLSchema
from socceraction.spadl import SPADLSchema
from socceraction.spadl.utils import add_names


def pytest_configure(config: Config) -> None:
    """Pytest configuration hook."""
    config.addinivalue_line("markers", "e2e: mark as end-to-end test.")


@pytest.fixture(scope='session')
def sb_worldcup_data() -> Iterator[pd.HDFStore]:
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


@pytest.fixture()
def shot() -> pd.DataFrame:
    return add_names(
        pd.DataFrame(
            [
                {
                    "game_id": 8658,
                    "original_event_id": "a8692197-bb35-453d-9191-fe7daa25f8df",
                    "period_id": 2,
                    "time_seconds": 1176.0,
                    "team_id": 771,
                    "player_id": 3009.0,
                    "start_x": 105 - 11,
                    "start_y": 34,
                    "end_x": 105.0,
                    "end_y": 37.01265822784811,
                    "type_id": 11,
                    "result_id": 1,
                    "bodypart_id": 0,
                }
            ]
        )
    )
