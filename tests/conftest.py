import os

import pandas as pd
import pytest


@pytest.fixture
def sb_worldcup_data() -> pd.HDFStore:
    hdf_file = os.path.join(
        os.path.dirname(__file__), 'data', 'statsbomb', 'spadl-WorldCup-2018.h5'
    )
    store = pd.HDFStore(hdf_file, mode='r')
    yield store
    store.close()
