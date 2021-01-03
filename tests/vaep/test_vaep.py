import pandas as pd

from socceraction.vaep import VAEP
from socceraction.vaep import features as fs


def test_predict(sb_worldcup_data):
    # Test the vAEP framework on the StatsBomb World Cup data
    model = VAEP()
    # comppute features and labels
    games = sb_worldcup_data['games']
    features = pd.concat(
        [
            model.compute_features(game, sb_worldcup_data[f'actions/game_{game.game_id}'])
            for game in games.iloc[:-1].itertuples()
        ]
    )
    expected_features = set(fs.feature_column_names(model.xfns, model.nb_prev_actions))
    assert set(features.columns) == expected_features
    labels = pd.concat(
        [
            model.compute_labels(game, sb_worldcup_data[f'actions/game_{game.game_id}'])
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
    actions = sb_worldcup_data[f'actions/game_{game.game_id}']
    ratings = model.rate(game, actions)
    expected_rating_columns = set(
        list(actions.columns) + ['offensive_value', 'defensive_value', 'vaep_value']
    )
    assert set(ratings.columns) == expected_rating_columns
