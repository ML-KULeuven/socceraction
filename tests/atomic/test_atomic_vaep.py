import pandas as pd
import pytest

import socceraction.atomic.spadl as atomicspadl
from socceraction.atomic.vaep import AtomicVAEP
from socceraction.atomic.vaep import features as fs


@pytest.mark.slow
def test_predict(sb_worldcup_data):
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
