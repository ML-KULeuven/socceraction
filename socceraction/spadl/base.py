import json
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

import pandas as pd  # type: ignore
import pandera as pa
import requests
from pandera.typing import DateTime, Series

from . import config as spadlconfig

import ssl; ssl._create_default_https_context = ssl._create_unverified_context


class MissingDataError(Exception):
    pass


def _remoteloadjson(path: str) -> List[Dict]:
    return requests.get(path).json()


def _localloadjson(path: str) -> List[Dict]:
    with open(path, "rt", encoding="utf-8") as fh:
        return json.load(fh)


class EventDataLoader(ABC):
    """
    Load event data either from a remote location or from a local folder.

    Parameters
    ----------
    root : str
        Root-path of the data.
    getter : str
        "remote" or "local"
    """

    def __init__(self, root, getter):
        self.root = root

        if getter == "remote":
            self.get = _remoteloadjson
        elif getter == "local":
            self.get = _localloadjson
        else:
            raise Exception("Invalid getter specified")

    @abstractmethod
    def competitions(self) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def games(self, competition_id: int, season_id: int) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def teams(self, game_id: int) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def players(self, game_id: int) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def events(self, game_id: int) -> pd.DataFrame:
        raise NotImplementedError


class CompetitionSchema(pa.SchemaModel):
    season_id: Series[int]
    season_name: Series[str]
    competition_id: Series[int]
    competition_name: Series[str]

    class Config:
        strict = True


class GameSchema(pa.SchemaModel):
    game_id: Series[int]
    season_id: Series[int]
    competition_id: Series[int]
    game_day: Series[int]
    game_date: Series[DateTime]
    home_team_id: Series[int]
    away_team_id: Series[int]

    class Config:
        strict = True


class TeamSchema(pa.SchemaModel):
    team_id: Series[int]
    team_name: Series[str]

    class Config:
        strict = True


class PlayerSchema(pa.SchemaModel):
    game_id: Series[int]
    team_id: Series[int]
    player_id: Series[int]
    player_name: Series[str]
    is_starter: Series[bool]
    minutes_played: Series[int]
    jersey_number: Series[int]

    class Config:
        strict = True


class EventSchema(pa.SchemaModel):
    game_id: Series[int]
    event_id: Series[int]
    period_id: Series[int]
    team_id: Series[int] = pa.Field(nullable=True)
    player_id: Series[int] = pa.Field(nullable=True)
    type_id: Series[int]
    type_name: Series[str]

    class Config:
        strict = True


class SPADLSchema(pa.SchemaModel):
    game_id: Series[int]
    action_id: Series[int]
    period_id: Series[int]
    timestamp: Optional[Series[str]]
    time_seconds: Series[float]
    team_id: Series[int]
    player_id: Series[int]
    start_x: Series[float]
    start_y: Series[float]
    end_x: Series[float]
    end_y: Series[float]
    bodypart_id: Series[int]
    bodypart_name: Optional[Series[str]]
    type_id: Series[int]
    type_name: Optional[Series[str]]
    result_id: Series[int]
    result_name: Optional[Series[str]]

    class Config:
        strict = True


def _fix_clearances(actions: pd.DataFrame) -> pd.DataFrame:
    next_actions = actions.shift(-1)
    next_actions[-1:] = actions[-1:]
    clearance_idx = actions.type_id == spadlconfig.actiontypes.index("clearance")
    actions.loc[clearance_idx, "end_x"] = next_actions[clearance_idx].start_x.values
    actions.loc[clearance_idx, "end_y"] = next_actions[clearance_idx].start_y.values

    return actions


def _fix_direction_of_play(actions: pd.DataFrame, home_team_id: int) -> pd.DataFrame:
    away_idx = (actions.team_id != home_team_id).values
    for col in ["start_x", "end_x"]:
        actions.loc[away_idx, col] = (
            spadlconfig.field_length - actions[away_idx][col].values
        )
    for col in ["start_y", "end_y"]:
        actions.loc[away_idx, col] = (
            spadlconfig.field_width - actions[away_idx][col].values
        )

    return actions


min_dribble_length: float = 3.0
max_dribble_length: float = 60.0
max_dribble_duration: float = 10.0


def _add_dribbles(actions: pd.DataFrame) -> pd.DataFrame:
    next_actions = actions.shift(-1)

    same_team = actions.team_id == next_actions.team_id
    # not_clearance = actions.type_id != actiontypes.index("clearance")

    dx = actions.end_x - next_actions.start_x
    dy = actions.end_y - next_actions.start_y
    far_enough = dx ** 2 + dy ** 2 >= min_dribble_length ** 2
    not_too_far = dx ** 2 + dy ** 2 <= max_dribble_length ** 2

    dt = next_actions.time_seconds - actions.time_seconds
    same_phase = dt < max_dribble_duration
    same_period = actions.period_id == next_actions.period_id

    dribble_idx = same_team & far_enough & not_too_far & same_phase & same_period

    dribbles = pd.DataFrame()
    prev = actions[dribble_idx]
    nex = next_actions[dribble_idx]
    dribbles["game_id"] = nex.game_id
    dribbles["period_id"] = nex.period_id
    dribbles["action_id"] = prev.action_id + 0.1
    dribbles["time_seconds"] = (prev.time_seconds + nex.time_seconds) / 2
    if "timestamp" in actions.columns:
        dribbles["timestamp"] = nex.timestamp
    dribbles["team_id"] = nex.team_id
    dribbles["player_id"] = nex.player_id
    dribbles["start_x"] = prev.end_x
    dribbles["start_y"] = prev.end_y
    dribbles["end_x"] = nex.start_x
    dribbles["end_y"] = nex.start_y
    dribbles["bodypart_id"] = spadlconfig.bodyparts.index("foot")
    dribbles["type_id"] = spadlconfig.actiontypes.index("dribble")
    dribbles["result_id"] = spadlconfig.results.index("success")

    actions = pd.concat([actions, dribbles], ignore_index=True, sort=False)
    actions = actions.sort_values(["game_id", "period_id", "action_id"]).reset_index(
        drop=True
    )
    actions["action_id"] = range(len(actions))
    return actions
