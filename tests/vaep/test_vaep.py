import pandas as pd
import pytest

from socceraction.vaep import VAEP
from socceraction.vaep import features as fs


@pytest.fixture(scope='session')
def vaep_model(sb_worldcup_data: pd.HDFStore) -> VAEP:
    # Test the vAEP framework on the StatsBomb World Cup data
    model = VAEP(nb_prev_actions=1)
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
    expected_labels = {'scores', 'concedes'}
    assert set(labels.columns) == expected_labels
    assert len(features) == len(labels)
    # fit the model
    model.fit(features, labels)
    return model


@pytest.mark.e2e
def test_predict(sb_worldcup_data: pd.HDFStore, vaep_model: VAEP) -> None:
    games = sb_worldcup_data['games']
    game = games.iloc[-1]
    actions = sb_worldcup_data[f'actions/game_{game.game_id}']
    ratings = vaep_model.rate(game, actions)
    expected_rating_columns = {'offensive_value', 'defensive_value', 'vaep_value'}
    assert set(ratings.columns) == expected_rating_columns


@pytest.mark.e2e
def test_predict_with_missing_features(sb_worldcup_data: pd.HDFStore, vaep_model: VAEP) -> None:
    games = sb_worldcup_data['games']
    game = games.iloc[-1]
    actions = sb_worldcup_data[f'actions/game_{game.game_id}']
    X = vaep_model.compute_features(game, actions)
    del X['period_id_a0']
    with pytest.raises(ValueError):
        vaep_model.rate(game, actions, X)


def vaep_model_with_empty_games(sb_worldcup_data: pd.HDFStore) -> VAEP:
    # Test the vAEP framework with empty games
    model = VAEP(nb_prev_actions=1)
    # Filter out games with empty event feed
    games_with_events = sb_worldcup_data['games']
    games_with_events = games_with_events[games_with_events['home_team_id'].notna()]
    # compute features
    features = pd.concat(
        [
            model.compute_features(game, sb_worldcup_data[f'actions/game_{game.game_id}'])
            for game in games_with_events.itertuples()
        ]
    )
    expected_features = set(fs.feature_column_names(model.xfns, model.nb_prev_actions))
    assert set(features.columns) == expected_features
    # fit the model with features
    model.fit(features)
    # compute labels
    labels = pd.concat(
        [
            model.compute_labels(game, sb_worldcup_data[f'actions/game_{game.game_id}'])
            for game in games_with_events.itertuples()
        ]
    )
    expected_labels = {'scores', 'concedes'}
    assert set(labels.columns) == expected_labels
    assert len(features) == len(labels)
    return model


def test_compute_labels_with_empty_games() -> None:
    model = VAEP(nb_prev_actions=1)
    # Define dummy empty games data
    games_data = pd.DataFrame({'game_id': [1, 2, 3], 'home_team_id': [100, 200, pd.NA]})
    # Define dummy empty actions data for all games
    actions_data = {
        1: pd.DataFrame(),  # No actions for game 1
        2: pd.DataFrame(),  # No actions for game 2
        3: pd.DataFrame(),  # No actions for game 3
    }
    # Test compute_labels with empty games
    empty_games = pd.DataFrame({'game_id': [3]})
    actions = pd.DataFrame()  # Provide dummy data for actions
    labels = model.compute_labels(empty_games.iloc[0], actions)
    assert labels.empty


def test_compute_features_with_empty_games() -> None:
    model = VAEP(nb_prev_actions=1)
    # add dummy data for games with empty action
    games_data = pd.DataFrame({'game_id': [1, 2, 3], 'home_team_id': [100, 200, pd.NA]})
    actions_data = {1: pd.DataFrame(), 2: pd.DataFrame(), 3: pd.DataFrame()}
    # Test compute_features with empty games
    empty_games = pd.DataFrame({'game_id': [3]})
    actions = pd.DataFrame()  # Provide empty dummy data for actions
    features = model.compute_features(empty_games.iloc[0], actions)
    assert features.empty


def test_rate_with_empty_games() -> None:
    model = VAEP(nb_prev_actions=1)
    games_data = pd.DataFrame({'game_id': [1, 2, 3], 'home_team_id': [100, 200, pd.NA]})
    actions_data = {1: pd.DataFrame(), 2: pd.DataFrame(), 3: pd.DataFrame()}
    # Test rate with empty games
    empty_games = pd.DataFrame({'game_id': [3]})
    actions = pd.DataFrame()  # Provide dummy data for actions
    ratings = model.rate(empty_games.iloc[0], actions)
    assert ratings.empty
