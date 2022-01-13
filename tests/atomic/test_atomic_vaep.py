import pandas as pd
import pytest

import socceraction.atomic.spadl as atomicspadl
import socceraction.atomic.spadl.config as spadlconfig
import socceraction.atomic.vaep.labels as lab
from socceraction.atomic.vaep import AtomicVAEP
from socceraction.atomic.vaep import features as fs


@pytest.fixture
def test_goal_df() -> pd.DataFrame:
    return pd.DataFrame(
        [spadlconfig.actiontypes.index('shot'), spadlconfig.actiontypes.index('goal')],
        columns=['type_id'],
    )


def test_atomic_goal_from_shot_label(test_goal_df: pd.DataFrame) -> None:
    assert (lab.goal_from_shot(test_goal_df) == pd.DataFrame([[True], [False]], columns=['goal']))[
        'goal'
    ].all()


@pytest.mark.e2e
def test_predict(sb_worldcup_data: pd.HDFStore) -> None:
    # Convert to atomic actions
    games = sb_worldcup_data['games']
    atomic_actions = {
        game.game_id: atomicspadl.convert_to_atomic(
            sb_worldcup_data[f'actions/game_{game.game_id}']
        )
        for game in games.itertuples()
    }
    # Test the vAEP framework on the StatsBomb World Cup data
    model = AtomicVAEP(nb_prev_actions=1)
    # comppute features and labels
    features = pd.concat(
        [
            model.compute_features(game, atomic_actions[game.game_id])
            for game in games.iloc[:-1].itertuples()
        ]
    )
    expected_features = set(fs.feature_column_names(model.xfns, model.nb_prev_actions))
    assert set(features.columns) == expected_features
    labels = pd.concat(
        [
            model.compute_labels(game, atomic_actions[game.game_id])
            for game in games.iloc[:-1].itertuples()
        ]
    )
    expected_labels = set(['scores', 'concedes'])
    assert set(labels.columns) == expected_labels
    assert len(features) == len(labels)
    # fit the model
    model.fit(features, labels)
    # rate a game
    game = games.iloc[-1]
    actions = atomic_actions[game.game_id]
    ratings = model.rate(game, actions)
    expected_rating_columns = {'offensive_value', 'defensive_value', 'vaep_value'}
    assert set(ratings.columns) == expected_rating_columns
