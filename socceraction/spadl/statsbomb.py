import json
import os
from typing import Dict, List, Tuple

import pandas as pd  # type: ignore
import socceraction.spadl.config as spadlconfig
from socceraction.spadl.base import EventDataLoader

_free_open_data: str = "https://raw.githubusercontent.com/statsbomb/open-data/master/data/"


class StatsBombLoader(EventDataLoader):
    """
    Load Statsbomb data either from a remote location
    (e.g., "https://raw.githubusercontent.com/statsbomb/open-data/master/data/")
    or from a local folder.

    This is a temporary class until statsbombpy* becomes compatible with socceraction
    https://github.com/statsbomb/statsbombpy
    """

    def __init__(self, root: str = _free_open_data, getter: str = "remote"):
        """
        Initalize the StatsBombLoader

        :param root: root-path of the data
        :param getter: "remote" or "local"
        """
        super().__init__(root, getter)

    def competitions(self) -> pd.DataFrame:
        return pd.DataFrame(self.get(os.path.join(self.root, "competitions.json")))

    def matches(self, competition_id: int, season_id: int) -> pd.DataFrame:
        path = os.path.join(self.root, f"matches/{competition_id}/{season_id}.json")
        return pd.DataFrame(_flatten(m) for m in self.get(path))

    def _lineups(self, match_id: int) -> List[Dict]:
        path = os.path.join(self.root, f"lineups/{match_id}.json")
        return self.get(path)

    def teams(self, match_id: int) -> pd.DataFrame:
        return pd.DataFrame(self._lineups(match_id))[["team_id", "team_name"]]

    def players(self, match_id: int) -> pd.DataFrame:
        return pd.DataFrame(
            _flatten_id(p)
            for lineup in self._lineups(match_id)
            for p in lineup["lineup"]
        )

    def events(self, match_id: int):
        eventsdf = pd.DataFrame(
            _flatten_id(e)
            for e in self.get(os.path.join(self.root, f"events/{match_id}.json"))
        )
        eventsdf["match_id"] = match_id
        return eventsdf


def _flatten_id(d: dict) -> dict:
    newd = {}
    extra = {}
    for k, v in d.items():
        if isinstance(v, dict):
            if len(v) == 2 and "id" in v and "name" in v:
                newd[k + "_id"] = v["id"]
                newd[k + "_name"] = v["name"]
            else:
                extra[k] = v
        else:
            newd[k] = v
    newd["extra"] = extra
    return newd


def _flatten(d: dict) -> dict:
    newd: dict = {}
    for k, v in d.items():
        if isinstance(v, dict):
            newd = {**newd, **_flatten(v)}
        else:
            newd[k] = v
    return newd


def extract_player_games(events: pd.DataFrame) -> pd.DataFrame:
    """
    Extract player games [player_id,game_id,minutes_played] from statsbomb match events
    """
    game_minutes = max(events[events.type_name == "Half End"].minute)

    game_id = events.match_id.mode().values[0]
    players = {}
    for startxi in events[events.type_name == "Starting XI"].itertuples():
        team_id, team_name = startxi.team_id, startxi.team_name
        for player in startxi.extra["tactics"]["lineup"]:
            player = _flatten_id(player)
            player = {
                **player,
                **{
                    "game_id": game_id,
                    "team_id": team_id,
                    "team_name": team_name,
                    "minutes_played": game_minutes,
                },
            }
            players[player["player_id"]] = player
    for substitution in events[events.type_name == "Substitution"].itertuples():
        replacement = substitution.extra["substitution"]["replacement"]
        replacement = {
            "player_id": replacement["id"],
            "player_name": replacement["name"],
            "minutes_played": game_minutes - substitution.minute,
            "team_id": substitution.team_id,
            "game_id": game_id,
            "team_name": substitution.team_name,
        }
        players[replacement["player_id"]] = replacement
        # minutes_played = substitution.minute
        players[substitution.player_id]["minutes_played"] = substitution.minute
    pg = pd.DataFrame(players.values()).fillna(0)
    for col in pg.columns:
        if "_id" in col:
            pg[col] = pg[col].astype(int)
    return pg


def convert_to_actions(events: pd.DataFrame, home_team_id: int) -> pd.DataFrame:
    """
    Convert StatsBomb events to SPADL actions
    """
    actions = pd.DataFrame()

    events["extra"] = events["extra"].fillna({})
    events = events.fillna(0)

    actions["game_id"] = events.match_id
    actions["period_id"] = events.period

    actions["time_seconds"] = (
        60 * events.minute + events.second
        - ((events.period > 1) * 45 * 60)
        - ((events.period > 2) * 45 * 60)
        - ((events.period > 3) * 15 * 60)
        - ((events.period > 4) * 15 * 60))
    actions["timestamp"] = events.timestamp
    actions["team_id"] = events.team_id
    actions["player_id"] = events.player_id

    actions["start_x"] = events.location.apply(lambda x: x[0] if x else 1)
    actions["start_y"] = events.location.apply(lambda x: x[1] if x else 1)
    actions["start_x"] = ((actions["start_x"] - 1) / 119) * spadlconfig.field_length
    actions["start_y"] = 68 - ((actions["start_y"] - 1) / 79) * spadlconfig.field_width

    end_location = events[["location", "extra"]].apply(_get_end_location, axis=1)
    actions["end_x"] = end_location.apply(lambda x: x[0] if x else 1)
    actions["end_y"] = end_location.apply(lambda x: x[1] if x else 1)
    actions["end_x"] = ((actions["end_x"] - 1) / 119) * spadlconfig.field_length
    actions["end_y"] = 68 - ((actions["end_y"] - 1) / 79) * spadlconfig.field_width

    actions["type_id"] = events[["type_name", "extra"]].apply(_get_type_id, axis=1)
    actions["result_id"] = events[["type_name", "extra"]].apply(_get_result_id, axis=1)
    actions["bodypart_id"] = events[["type_name", "extra"]].apply(
        _get_bodypart_id, axis=1
    )

    actions = (
        actions[actions.type_id != spadlconfig.actiontypes.index("non_action")]
        .sort_values(["game_id", "period_id", "time_seconds", "timestamp"])
        .reset_index(drop=True)
    )
    actions = _fix_direction_of_play(actions, home_team_id)
    actions = _fix_clearances(actions)

    actions["action_id"] = range(len(actions))
    actions = _add_dribbles(actions)

    for col in actions.columns:
        if "_id" in col:
            actions[col] = actions[col].astype(int)
    return actions


Location = Tuple[float, float]


def _get_end_location(q: Tuple[Location, dict]) -> Location:
    start_location, extra = q
    for event in ["pass", "shot", "carry"]:
        if event in extra and "end_location" in extra[event]:
            return extra[event]["end_location"]
    return start_location


def _get_type_id(q: Tuple[str, dict]) -> int:
    t, extra = q
    a = "non_action"
    if t == "Pass":
        a = "pass"  # default
        p = extra.get("pass", {})
        ptype = p.get("type", {}).get("name")
        height = p.get("height", {}).get("name")
        cross = p.get("cross")
        if ptype == "Free Kick":
            if height == "High Pass" or cross:
                a = "freekick_crossed"
            else:
                a = "freekick_short"
        elif ptype == "Corner":
            if height == "High Pass" or cross:
                a = "corner_crossed"
            else:
                a = "corner_short"
        elif ptype == "Goal Kick":
            a = "goalkick"
        elif ptype == "Throw-in":
            a = "throw_in"
        elif cross:
            a = "cross"
        else:
            a = "pass"
    elif t == "Dribble":
        a = "take_on"
    elif t == "Carry":
        a = "dribble"
    elif t == "Foul Committed":
        a = "foul"
    elif t == "Duel" and extra.get("duel", {}).get("type", {}).get("name") == "Tackle":
        a = "tackle"
    elif t == "Interception":
        a = "interception"
    elif t == "Shot":
        extra_type = extra.get("shot", {}).get("type", {}).get("name")
        if extra_type == "Free Kick":
            a = "shot_freekick"
        elif extra_type == "Penalty":
            a = "shot_penalty"
        else:
            a = "shot"
    elif t == "Own Goal Against":
        a = "shot"
    elif t == "Goal Keeper":
        extra_type = extra.get("goalkeeper", {}).get("type", {}).get("name")
        if extra_type == "Shot Saved":
            a = "keeper_save"
        elif extra_type == "Collected" or extra_type == "Keeper Sweeper":
            a = "keeper_claim"
        elif extra_type == "Punch":
            a = "keeper_punch"
        else:
            a = "non_action"
    elif t == "Clearance":
        a = "clearance"
    elif t == "Miscontrol":
        a = "bad_touch"
    else:
        a = "non_action"
    return spadlconfig.actiontypes.index(a)


def _get_result_id(q: Tuple[str, dict]) -> int:
    t, x = q

    if t == "Pass":
        pass_outcome = x.get("pass", {}).get("outcome", {}).get("name")
        if pass_outcome in ["Incomplete", "Out"]:
            r = "fail"
        elif pass_outcome == "Pass Offside":
            r = "offside"
        else:
            r = "success"
    elif t == "Shot":
        shot_outcome = x.get("shot", {}).get("outcome", {}).get("name")
        if shot_outcome == "Goal":
            r = "success"
        elif shot_outcome in ["Blocked", "Off T", "Post", "Saved", "Wayward"]:
            r = "fail"
        else:
            r = "fail"
    elif t == "Dribble":
        dribble_outcome = x.get("dribble", {}).get("outcome", {}).get("name")
        if dribble_outcome == "Incomplete":
            r = "fail"
        elif dribble_outcome == "Complete":
            r = "success"
        else:
            r = "success"
    elif t == "Foul Committed":
        foul_card = x.get("foul_committed", {}).get("card", {}).get("name", "")
        if "Yellow" in foul_card:
            r = "yellow_card"
        elif "Red" in foul_card:
            r = "red_card"
        else:
            r = "success"
    elif t == "Duel":
        duel_outcome = x.get("duel", {}).get("outcome", {}).get("name")
        if duel_outcome in ["Lost In Play", "Lost Out"]:
            r = "fail"
        elif duel_outcome in ["Success in Play", "Won"]:
            r = "success"
        else:
            r = "success"
    elif t == "Interception":
        interception_outcome = x.get("interception", {}).get("outcome", {}).get("name")
        if interception_outcome in ["Lost In Play", "Lost Out"]:
            r = "fail"
        elif interception_outcome == "Won":
            r = "success"
        else:
            r = "success"
    elif t == "Own Goal Against":
        r = "owngoal"
    elif t == "Goal Keeper":
        goalkeeper_outcome = x.get("goalkeeper", {}).get("outcome", {}).get("name", "x")
        if goalkeeper_outcome in [
            "Claim",
            "Clear",
            "Collected Twice",
            "In Play Safe",
            "Success",
            "Touched Out",
        ]:
            r = "success"
        elif goalkeeper_outcome in ["In Play Danger", "No Touch"]:
            r = "fail"
        else:
            r = "success"
    elif t == "Clearance":
        r = "success"
    elif t == "Miscontrol":
        r = "fail"
    else:
        r = "success"

    return spadlconfig.results.index(r)


def _get_bodypart_id(q: Tuple[str, dict]) -> int:
    t, x = q
    if t == "Shot":
        bp = x.get("shot", {}).get("body_part", {}).get("name")
    elif t == "Pass":
        bp = x.get("pass", {}).get("body_part", {}).get("name")
    elif t == "Goal Keeper":
        bp = x.get("goalkeeper", {}).get("body_part", {}).get("name")
    else:
        bp = None

    if bp is None:
        b = "foot"
    elif "Head" in bp:
        b = "head"
    elif "Foot" in bp or bp == "Drop Kick":
        b = "foot"
    else:
        b = "other"

    return spadlconfig.bodyparts.index(b)


def _fix_clearances(actions: pd.DataFrame) -> pd.DataFrame:
    next_actions = actions.shift(-1)
    next_actions[-1:] = actions[-1:]
    clearance_idx = actions.type_id == spadlconfig.actiontypes.index("clearance")
    actions.loc[clearance_idx, "end_x"] = next_actions[clearance_idx].start_x.values
    actions.loc[clearance_idx, "end_y"] = next_actions[clearance_idx].start_y.values

    return actions


def _fix_direction_of_play(actions: pd.DataFrame, home_team_id: int) -> int:
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
