"""Implements the label tranformers of the VAEP framework."""

from collections.abc import Set
from functools import reduce
from typing import Any, Literal, Optional, cast

import pandas as pd
from pandera.typing import DataFrame

import socceraction.spadl.config as spadl
from socceraction.features.utils import feature_generator
from socceraction.types import Features, Mask, SPADLActions
from socceraction.utils import deprecated

_non_possessing_actions = {
    "clearance",
    "keeper_save",
    "keeper_punch",
    "foul",
    "tackle",
    "interception",
}


def create_scores_label(
    name: str = "scores",
    mode: Literal["window", "possession"] = "window",
    nr_actions: Optional[int] = None,
    nr_seconds: Optional[int] = None,
    non_possessing_actions: Optional[Set[str]] = _non_possessing_actions,
):
    """Create a binary goal/no-goal label generator.

    Supports the following modes:
      - action-based window: consider the next `nr_actions` after the current action.
      - time-based window: consider the next actions within `nr_seconds` after the current action.
      - possession-based window: consider the next actions within the same possession after the current action.

    Parameters
    ----------
    mode : {'window', 'possession'}, default='window'  # noqa: DAR103
        How to determine the next actions to consider.
    nr_actions : int, default=10  # noqa: DAR103
        Number of actions after the current action to consider.
        Only used when mode='window'.
    nr_seconds : int, default=None  # noqa
        Only consider actions that occur within nr_seconds seconds of the
        current action. Only used when mode='window'.
    non_possessing_actions: list[str]
        List of actions that do not result in a possession change.
        Only used when mode='possession'. Defaults to
        {'clearance', 'keeper_save', 'keeper_punch', 'foul', 'tackle', 'interception'}

    """

    def _scores(
        actions: SPADLActions,
        mask: Mask,
    ) -> Features:
        """Determine whether the team possessing the ball scored a goal within the next x actions, seconds or possession.

        Parameters
        ----------
        actions : SPADLActions
            The actions of a game.
        mask : Mask
            A boolean mask to filter actions.

        Returns
        -------
        Features
            A dataframe with a column 'scores' and a row for each action set to
            True if a goal was scored by the team possessing the ball within the
            next x actions; otherwise False.
        """
        nonlocal nr_actions, nr_seconds, non_possessing_actions

        # merging goals, owngoals and team_ids
        goals = actions["type_name"].str.contains("shot") & (
            actions["result_id"] == spadl.results.index("success")
        )
        owngoals = actions["type_name"].str.contains("shot") & (
            actions["result_id"] == spadl.results.index("owngoal")
        )
        y = pd.concat([goals, owngoals, actions["team_id"]], axis=1)
        y.columns = ["goal", "owngoal", "team_id"]

        res = pd.Series(False, index=actions.index)  # Initialize a Series with False values
        res = res | y["goal"]

        # adding future results
        if mode.lower() == "window":
            if nr_actions is None and nr_seconds is None:
                nr_actions = 10
            elif nr_actions is not None and nr_seconds is not None:
                raise ValueError("Only one of nr_actions or nr_seconds should be set")

            if nr_actions is not None:
                for i in range(1, nr_actions):
                    for c in ["team_id", "goal", "owngoal"]:
                        shifted = y[c].shift(-i)
                        shifted[-i:] = y[c].iloc[len(y) - 1]
                        y["%s+%d" % (c, i)] = shifted

                for i in range(1, nr_actions):
                    gi = y["goal+%d" % i] & (y["team_id+%d" % i] == y["team_id"])
                    ogi = y["owngoal+%d" % i] & (y["team_id+%d" % i] != y["team_id"])
                    res = res | gi | ogi

            elif nr_seconds is not None:
                for i in range(len(actions)):
                    possession_team = actions["team_id"].iloc[i]
                    start_time = actions["time_seconds"].iloc[i]
                    j = i + 1

                    # Search for subsequent actions
                    # time restarts at 0 in every half, so by taking the absolute value,
                    # you dont look at goals scored in the 2nd half (with a lower time in seconds)
                    while (
                        j < len(actions)
                        and abs(actions["time_seconds"].iloc[j] - start_time) < nr_seconds
                    ):
                        if (
                            bool(y["goal"].iloc[j]) and y["team_id"].iloc[j] == possession_team
                        ):  # team possessing the ball scored a goal
                            res.iloc[i] = True
                            break
                        elif (
                            bool(y["owngoal"].iloc[j]) and y["team_id"].iloc[j] != possession_team
                        ):  # other team conceded an owngoal
                            res.iloc[i] = True
                            break
                        j += 1

        elif mode.lower() == "possession":
            if non_possessing_actions is None:
                non_possessing_actions = set()

            for i in range(len(actions)):
                possession_team = actions["team_id"].iloc[i]
                j = i + 1

                # Search for subsequent actions in the same possession
                while j < len(actions) and (
                    actions["team_id"].iloc[j] == possession_team
                    or actions["type_name"].iloc[j] in non_possessing_actions
                ):
                    if bool(y["goal"].iloc[j]):
                        res.iloc[i] = True
                        break
                    j += 1
        else:
            raise ValueError(f"Invalid mode: {mode}")

        return cast(DataFrame[Any], pd.DataFrame(res, columns=[name]).loc[mask])

    return feature_generator("actions", features=[name])(_scores)


scores = create_scores_label("scores", mode="window", nr_actions=10)


def create_concedes_label(
    name: str = "scores",
    mode: Literal["window", "possession"] = "window",
    nr_actions: Optional[int] = None,
    nr_seconds: Optional[int] = None,
    non_possessing_actions: Optional[Set[str]] = _non_possessing_actions,
):
    """Create a binary goal/no-goal label generator.

    Supports the following modes:
      - action-based window: consider the next `nr_actions` after the current action.
      - time-based window: consider the next actions within `nr_seconds` after the current action.
      - possession-based window: consider the next actions within the same possession after the current action.

    Parameters
    ----------
    mode : {'window', 'possession'}, default='window'  # noqa: DAR103
        How to determine the next actions to consider.
    nr_actions : int, default=10  # noqa: DAR103
        Number of actions after the current action to consider.
        Only used when mode='window'.
    nr_seconds : int, default=None  # noqa
        Only consider actions that occur within nr_seconds seconds of the
        current action. Only used when mode='window'.
    non_possessing_actions: list[str]
        List of actions that do not result in a possession change.
        Only used when mode='possession'. Defaults to
        {'clearance', 'keeper_save', 'keeper_punch', 'foul', 'tackle', 'interception'}

    """

    def _concedes(
        actions: SPADLActions,
        mask: Mask,
        mode: Literal["window", "possession"] = "window",
    ) -> Features:
        """Determine whether the team possessing the ball conceded a goal within the next x actions.

        Supports the following modes:
        - action-based window: consider the next `nr_actions` after the current action.
        - time-based window: consider the next actions within `nr_seconds` after the current action.
        - possession-based window: consider the next actions within the same possession after the current action.

        Parameters
        ----------
        actions : SPADLActions
            The actions of a game.
        mask : Mask
            A boolean mask to filter actions.

        Returns
        -------
        Features
            A dataframe with a column 'concedes' and a row for each action set to
            True if a goal was conceded by the team possessing the ball within the
            next x actions; otherwise False.
        """
        nonlocal nr_actions, nr_seconds, non_possessing_actions

        # merging goals,owngoals and team_ids
        goals = actions["type_name"].str.contains("shot") & (
            actions["result_id"] == spadl.results.index("success")
        )
        owngoals = actions["type_name"].str.contains("shot") & (
            actions["result_id"] == spadl.results.index("owngoal")
        )
        y = pd.concat([goals, owngoals, actions["team_id"]], axis=1)
        y.columns = ["goal", "owngoal", "team_id"]

        # adding future results
        res = pd.Series(False, index=actions.index)  # Initialize a Series with False values
        res = res | y["goal"]

        if mode.lower() == "window":
            if nr_actions is not None:
                for i in range(1, nr_actions):
                    for c in ["team_id", "goal", "owngoal"]:
                        shifted = y[c].shift(-i)
                        shifted[-i:] = y[c].iloc[len(y) - 1]
                        y["%s+%d" % (c, i)] = shifted

                for i in range(1, nr_actions):
                    gi = y["goal+%d" % i] & (y["team_id+%d" % i] != y["team_id"])
                    ogi = y["owngoal+%d" % i] & (y["team_id+%d" % i] == y["team_id"])
                    res = res | gi | ogi

            elif nr_seconds is not None:
                for i in range(len(actions)):
                    possession_team = actions["team_id"].iloc[i]
                    start_time = actions["time_seconds"].iloc[i]
                    j = i + 1

                    # Search for subsequent actions in the same possession
                    # time restarts at 0 in every half, so by taking the absolute value,
                    # you dont look at goals scored in the 2nd half (with a lower time in seconds)
                    while (
                        j < len(actions)
                        and abs(actions["time_seconds"].iloc[j] - start_time) < nr_seconds
                    ):
                        if (
                            bool(y["goal"].iloc[j]) and y["team_id"].iloc[j] != possession_team
                        ):  # other team scored a goal
                            res.iloc[i] = True
                            break
                        elif (
                            bool(y["owngoal"].iloc[j]) and y["team_id"].iloc[j] == possession_team
                        ):  # team conceded an owngoal
                            res.iloc[i] = True
                            break
                        j += 1

        elif mode.lower() == "possession":
            if non_possessing_actions is None:
                non_possessing_actions = set()

            for i in range(len(actions)):
                possession_team = actions["team_id"][i]
                j = i + 1

                # Search for subsequent actions in the same possession to look for own goals
                while j < len(actions) and (
                    actions["team_id"][j] == possession_team
                    or actions["type_name"][j] in non_possessing_actions
                ):
                    if res[j]:
                        res.iloc[i] = True
                        break
                    j += 1

            # now check if opponent scores in the next possession
            for i in range(len(actions)):
                possession_team = actions["team_id"][i]
                j = i + 1
                # Wait for possession switching teams
                while j < len(actions) and (
                    actions["team_id"][j] == possession_team
                    or actions["type_name"][j] in non_possessing_actions
                ):
                    j += 1

                # if not at end of game
                if j < len(actions):
                    possession_team = actions["team_id"][j]  # other team has now the ball
                    # Search for subsequent actions in the same possession
                    while j < len(actions) and (
                        actions["team_id"][j] == possession_team
                        or actions["type_name"][j] in non_possessing_actions
                    ):
                        if bool(y["goal"][j]):
                            res.iloc[i] = True
                            break
                        j += 1

        return pd.DataFrame(res, columns=[name]).loc[mask]

    return feature_generator("actions", features=[name])(_concedes)


concedes = create_concedes_label("concedes", mode="window", nr_actions=10)


@feature_generator("actions", features=["scores_xg"])
def scores_xg(actions: SPADLActions, mask: Mask, nr_actions: int = 10) -> Features:
    """Determine the xG value generated by the team possessing the ball within the next x actions.

    Parameters
    ----------
    actions : SPADLActions
        The actions of a game.
    mask : Mask
        A boolean mask to filter actions.
    nr_actions : int, default=10  # noqa: DAR103
        Number of actions after the current action to consider.

    Returns
    -------
    Features
        A dataframe with a column 'scores_xg' and a row for each action set to
        the total xG value generated by the team possessing the ball within the
        next x actions.
    """
    y = actions.loc[:, ["xg", "team_id"]].fillna(0)
    y.columns = ["shot", "team_id"]

    # adding future results
    for i in range(1, nr_actions):
        for c in ["team_id", "shot"]:
            shifted = y[c].shift(-i)
            shifted[-i:] = 0.0
            y["%s+%d" % (c, i)] = shifted

    # removing opponent shots
    for i in range(1, nr_actions):
        y.loc[(y["team_id+%d" % i] != y["team_id"]), "shot+%d" % i] = 0

    # combine multiple shots in possession
    # see https://fbref.com/en/expected-goals-model-explained
    y["sum"] = 1
    y["scores_xg"] = 1 - y[["sum", "shot"] + ["shot+%d" % i for i in range(1, nr_actions)]].apply(
        lambda shots: reduce(lambda agg, xg: agg * (1 - xg), shots), axis=1
    )
    return y.loc[mask, ["scores_xg"]]


@feature_generator("actions", features=["concedes_xg"])
def concedes_xg(actions: pd.DataFrame, mask: Mask, nr_actions: int = 10) -> Features:
    """Determine the xG value conceded by the team possessing the ball within the next x actions.

    Parameters
    ----------
    actions : pd.DataFrame
        The actions of a game.
    mask : Mask
        A boolean mask to filter actions.
    nr_actions : int, default=10  # noqa: DAR103
        Number of actions after the current action to consider.

    Returns
    -------
    Features
        A dataframe with a column 'concedes_xg' and a row for each action set to
        the total xG value conceded by the team possessing the ball within the
        next x actions.
    """
    y = actions.loc[:, ["xg", "team_id"]].fillna(0)
    y.columns = ["shot", "team_id"]

    # adding future results
    for i in range(1, nr_actions):
        for c in ["team_id", "shot"]:
            shifted = y[c].shift(-i)
            shifted[-i:] = 0.0
            y["%s+%d" % (c, i)] = shifted

    # removing created shots
    for i in range(1, nr_actions):
        y.loc[(y["team_id+%d" % i] == y["team_id"]), "shot+%d" % i] = 0

    # combine multiple shots in possession
    # see https://fbref.com/en/expected-goals-model-explained
    y["sum"] = 1
    y["concedes_xg"] = 1 - y[["sum"] + ["shot+%d" % i for i in range(1, nr_actions)]].apply(
        lambda shots: reduce(lambda agg, xg: agg * (1 - xg), shots), axis=1
    )
    return y.loc[mask, ["concedes_xg"]]


@deprecated("Use socceraction.xg.labels.goal_from_shot instead.")
def goal_from_shot(actions: SPADLActions, mask: Mask) -> Features:
    """See socceraction.xg.labels.goal_from_shot."""
    from socceraction.xg.labels import goal_from_shot

    return goal_from_shot
