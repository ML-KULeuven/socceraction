"""StatsBomb-specific attributes."""

import math
from typing import Callable, Optional

import pandas as pd
from pandera.typing import DataFrame
from socceraction.data.providers.statsbomb import StatsBombEventSchema
from socceraction.features.utils import feature_generator
from socceraction.spadl import config as spadlcfg
from socceraction.types import Features, Mask

from .goal_face_occlusion import (
    mean_occlusion_percent,
    occlusion_binary,
    occlusion_gaps,
    occlusion_numeric,
)
from .shot_blocking import (
    gaussian_mass_right_of_x,
    is_inside_triangle,
    is_inside_triangle_smoothed,
    right_of_x,
)

_spadl_cfg = {
    "length": 105,
    "width": 68,
    "penalty_box_length": 16.5,
    "penalty_box_width": 40.3,
    "six_yard_box_length": 5.5,
    "six_yard_box_width": 18.3,
    "goal_width": 7.32,
    "penalty_spot_distance": 11,
    "goal_length": 2,
    "origin_x": 0,
    "origin_y": 0,
    "circle_radius": 9.15,
}


def _sb_to_spadl(sb_x: float, sb_y: float) -> tuple[float, float]:
    spadl_x = ((sb_x - 1) / 119) * spadlcfg.field_length
    spadl_y = spadlcfg.field_width - ((sb_y - 1) / 79) * spadlcfg.field_width
    return spadl_x, spadl_y


def _mask_shots(events: DataFrame[StatsBombEventSchema]) -> Mask:
    return events["extra"].apply(lambda x: "shot" in x).to_numpy(dtype=bool)


@feature_generator("events", features=["from_counterattack"])
def statsbomb_counterattack(events: DataFrame[StatsBombEventSchema], mask: Mask) -> Features:
    """Get whether an action was from a counterattack.

    This is derived from the 'play_pattern' annotation in StatsBomb's event
    stream data.

    Parameters
    ----------
    events : DataFrame[StatsBombEventSchema]
        The StatsBomb events of a game.
    mask : Mask
        A boolean mask to select the actions for which attributes should be
        computed.

    Returns
    -------
    pd.DataFrame
    """
    # Create the output DataFrame
    output = pd.DataFrame(index=events.loc[mask].index)
    output["from_counterattack"] = events.loc[mask, "play_pattern_name"] == "From Counter"

    return output


@feature_generator("events", features=["under_pressure"])
def statsbomb_under_pressure(events: DataFrame[StatsBombEventSchema], mask: Mask) -> Features:
    """Get whether the player was under pressure when performing an action.

    This is derived from the 'under_pressure' annotation in StatsBomb's event
    stream data.

    Parameters
    ----------
    events : DataFrame[StatsBombEventSchema]
        The StatsBomb events of a game.
    mask : Mask
        A boolean mask to select the actions for which attributes should be
        computed.

    Returns
    -------
    pd.DataFrame
    """
    return events.loc[mask, ["under_pressure"]]


@feature_generator("events", features=["shot_statsbomb_xg"])
def statsbomb_xg(events: DataFrame[StatsBombEventSchema], mask: Mask) -> Features:
    """Get the xG value of a shot.

    Parameters
    ----------
    events : DataFrame[StatsBombEventSchema]
        The StatsBomb events of a game.
    mask : Mask
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    pd.DataFrame
    """
    # Filter rows where 'shot' exists in 'extra'
    mask_shots = _mask_shots(events)

    # Create the output DataFrame with 'statsbomb_xg' values and NaNs for others
    output = pd.DataFrame({"statsbomb_xg": pd.NA}, index=events.loc[mask].index)

    # Extract 'statsbomb_xg' where 'shot' exists
    statsbomb_xg_values = events.loc[mask & mask_shots, "extra"].apply(
        lambda x: x["shot"]["statsbomb_xg"]
    )
    output.loc[mask_shots, "shot_statsbomb_xg"] = statsbomb_xg_values.values

    return output


@feature_generator("events", features=["shot_open_goal"])
def statsbomb_shot_open_goal(events: DataFrame[StatsBombEventSchema], mask: Mask) -> Features:
    """Get whether a shot was taken into an open goal.

    This is derived from the 'open_goal' annotation in StatsBomb's event
    stream data.

    Parameters
    ----------
    events : DataFrame[StatsBombEventSchema]
        The StatsBomb events of a game.
    mask : Mask
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    pd.DataFrame
    """
    # Filter rows where 'shot' exists in 'extra'
    mask_shots = _mask_shots(events)

    # Create the output DataFrame with 'open_goal' for shots and NaNs for others
    output = pd.DataFrame(
        data={"open_goal": pd.Series(dtype="boolean", index=events.loc[mask].index)},
        index=events.loc[mask].index,
    )

    # Extract 'open_goal' where 'shot' exists
    statsbomb_open_goal = events.loc[mask & mask_shots, "extra"].apply(
        lambda x: "open_goal" in x["shot"]
    )
    output.loc[mask_shots, "shot_open_goal"] = statsbomb_open_goal.values

    return output


@feature_generator("events", features=["shot_one_on_one"])
def statsbomb_shot_one_on_one(events: DataFrame[StatsBombEventSchema], mask: Mask) -> Features:
    """Get whether the shooter was one-on-one with the goalkeeper when shooting.

    This is derived from the 'one_on_one' annotation in StatsBomb's event
    stream data.

    Parameters
    ----------
    events : DataFrame[StatsBombEventSchema]
        The StatsBomb events of a game.
    mask : Mask
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    pd.DataFrame
    """
    # Filter rows where 'shot' exists in 'extra'
    mask_shots = _mask_shots(events)

    # Create the output DataFrame with 'open_goal' for shots and NaNs for others
    output = pd.DataFrame(
        data={"shot_one_on_one": pd.Series(dtype="boolean", index=events.loc[mask].index)},
        index=events.loc[mask].index,
    )

    # Extract 'open_goal' where 'shot' exists
    statsbomb_one_on_one = events.loc[mask & mask_shots, "extra"].apply(
        lambda x: "one_on_one" in x["shot"]
    )
    output.loc[mask_shots, "shot_one_on_one"] = statsbomb_one_on_one.values

    return output


@feature_generator("events", features=["shot_first_touch"])
def statsbomb_shot_first_touch(events: DataFrame[StatsBombEventSchema], mask: Mask) -> Features:
    """Get whether the shot was a first-touch shot.

    This is derived from the 'first_time' annotation in StatsBomb's event
    stream data.

    Parameters
    ----------
    events : DataFrame[StatsBombEventSchema]
        The StatsBomb events of a game.
    mask : Mask
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    pd.DataFrame
    """
    # Filter rows where 'shot' exists in 'extra'
    mask_shots = _mask_shots(events)

    # Create the output DataFrame with 'open_goal' for shots and NaNs for others
    output = pd.DataFrame(
        data={"first_touch": pd.Series(dtype="boolean", index=events.loc[mask].index)},
        index=events.loc[mask].index,
    )

    # Extract 'open_goal' where 'shot' exists
    statsbomb_first_touch = events.loc[mask & mask_shots, "extra"].apply(
        lambda x: "first_time" in x["shot"]
    )
    output.loc[mask_shots, "shot_first_touch"] = statsbomb_first_touch.values

    return output


def create_statsbomb_goal_face_occlusion_feature(
    smoothed: bool = False,
    defender_width: float = 0.8,
    goalkeeper_width: float = 1.6,
    defender_sigma: Optional[float] = None,
    goalkeeper_sigma: Optional[float] = None,
) -> Callable[[DataFrame[StatsBombEventSchema], Mask], Features]:
    """Create a feature generator that computes the goal face occlusion on a shot.

    Parameters
    ----------
    smoothed : bool, optional
        Whether to compute a smoothed version of the feature by
        representing defenders as 2D bell-shaped surfaces (2D Gaussian
        distributions) instead of fixed-radius circles.
    defender_width : float, optional
        Width assigned to outfield defenders (meters)
    goalkeeper_width : float, optional
        Width assigned to goalkeepers (meters)
    defender_sigma : float, optional
        Standard deviation for the Gaussian representation of outfield
        defenders. If None, it is set to defender_width / 2.355.
    goalkeeper_sigma : float, optional
        Standard deviation for the Gaussian representation of goalkeepers.
        If None, it is set to goalkeeper_width / 2.355.
    """
    # If defender_sigma or goalkeeper_sigma are None, set them to width / 2.355
    # as such the Gaussian covers about the same width as the binary segment
    if defender_sigma is None:
        defender_sigma = defender_width / 2.355
    if goalkeeper_sigma is None:
        goalkeeper_sigma = goalkeeper_width / 2.355

    def feature_func(events: DataFrame[StatsBombEventSchema], mask: Mask) -> Features:
        """Get the free projection area.

        This feature represents the proportion of the goal that is left
        uncovered by the goalkeeper and defenders. To capture the fact that
        players are not static and will react to the shot, we assume that the
        defending player has an effective span of one arm length (80 cm) and
        the goalkeeper has an effect of two arm lengths (160 cm).

        Parameters
        ----------
        events : DataFrame[StatsBombEventSchema]
            The StatsBomb events of a game.
        mask : Mask
            A boolean mask to select the shots for which attributes should be
            computed.

        Returns
        -------
        Features
            A dataframe with a column containing the free projection area
            ('shot_goal_face_occlusion_pct') and the number of gaps in the
            projection area ('shot_goal_face_occlusion_gaps').

        References
        ----------
        .. [1] Cem Arslan, Pieter Robberechts and Jesse Davis. "Enhancing xG
           Models with Freeze Frame Data". DTAI Sports Analytics Lab, Sept 2, 2020.
           https://dtai.cs.kuleuven.be/sports/blog/enhancing-xg-models-with-freeze-frame-data
        .. [2] Dinesh Vatvani. "Upgrading Expected Goals". StatsBomb, May 15, 2022.
           https://blogarchive.statsbomb.com/articles/soccer/upgrading-expected-goals/
        """
        # Filter rows where 'shot' exists in 'extra'
        mask_shots = _mask_shots(events)

        # Create the output DataFrame
        output = pd.DataFrame(
            data={
                "shot_goal_face_occlusion_gaps": pd.Series(
                    dtype="Int64", index=events.loc[mask].index
                ),
                "shot_goal_face_occlusion_pct": pd.Series(
                    dtype="float", index=events.loc[mask].index
                ),
            },
            index=events.loc[mask].index,
        )

        # We have to use StatsBomb coordinates here
        for idx, shot in events.loc[mask & mask_shots].iterrows():
            if "shot" not in shot.extra or "freeze_frame" not in shot.extra["shot"]:
                # No freeze frame data available for this shot
                continue
            freeze_frame = shot.extra["shot"]["freeze_frame"]
            if len(shot.location) == 2:
                start_x, start_y = shot.location
            elif len(shot.location) == 3:
                start_x, start_y, _ = shot.location
            else:
                # Not a valid location
                continue
            # Now we remove the area blocked by each defending player
            defenders = [
                {
                    "location": player["location"],
                    "position": player["position"]["name"],
                }
                for player in freeze_frame
                if not player["teammate"]
            ]
            # By default, the entire goal is free
            if smoothed:
                occlusion, _ = occlusion_numeric(
                    (start_x, start_y),
                    defenders,
                    goalkeeper_sigma=goalkeeper_sigma,
                    defender_sigma=defender_sigma,
                )
            else:
                occlusion, _ = occlusion_binary(
                    (start_x, start_y),
                    defenders,
                    defender_width=defender_width,
                    goalkeeper_width=goalkeeper_width,
                )
            output.loc[idx, "shot_goal_face_occlusion_gaps"] = occlusion_gaps(occlusion)
            output.loc[idx, "shot_goal_face_occlusion_pct"] = mean_occlusion_percent(occlusion)

        return output

    return feature_func


statsbomb_shot_goal_face_occlusion = feature_generator(
    "events", features=["shot_goal_face_occlusion_pct", "shot_goal_face_occlusion_gaps"]
)(create_statsbomb_goal_face_occlusion_feature(smoothed=False))
statsbomb_shot_goal_face_occlusion_smoothed = feature_generator(
    "events", features=["shot_goal_face_occlusion_pct", "shot_goal_face_occlusion_gaps"]
)(create_statsbomb_goal_face_occlusion_feature(smoothed=True))


@feature_generator(
    "events",
    features=[
        "shot_goalkeeper_x",
        "shot_goalkeeper_y",
        "shot_goalkeeper_dist_to_ball",
        "shot_goalkeeper_dist_to_goal",
        "shot_goalkeeper_angle_to_goal",
    ],
)
def statsbomb_shot_goalkeeper_position(
    events: DataFrame[StatsBombEventSchema], mask: Mask
) -> Features:
    """Get the goalkeeper's position.

    Parameters
    ----------
    events : DataFrame[StatsBombEventSchema]
        The StatsBomb events of a game.
    mask : Mask
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    Features
        A dataframe with a column containing the goalkeeper's x-coordinate
        ('shot_goalkeeper_x'), y-coordinate ('shot_goalkeeper_y'), distance to the ball
        ('shot_goalkeeper_dist_to_ball'), distance to the center of the goal
        ('shot_goalkeeper_dist_to_goal') and the angle to the center of the goal
        ('shot_goalkeeper_angle_to_goal').
    """
    # Filter rows where 'shot' exists in 'extra'
    mask_shots = _mask_shots(events)

    # Create the output DataFrame
    output = pd.DataFrame(
        data={
            "shot_goalkeeper_x": pd.Series(dtype="float", index=events.loc[mask].index),
            "shot_goalkeeper_y": pd.Series(dtype="float", index=events.loc[mask].index),
            "shot_goalkeeper_dist_to_ball": pd.Series(dtype="float", index=events.loc[mask].index),
            "shot_goalkeeper_dist_to_goal": pd.Series(dtype="float", index=events.loc[mask].index),
            "shot_goalkeeper_angle_to_goal": pd.Series(
                dtype="float", index=events.loc[mask].index
            ),
        },
        index=events.loc[mask].index,
    )

    for idx, shot in events.loc[mask & mask_shots].iterrows():
        if "shot" not in shot.extra or "freeze_frame" not in shot.extra["shot"]:
            # No freeze frame data available for this shot
            continue
        freeze_frame = shot.extra["shot"]["freeze_frame"]
        goalkeeper = next(
            (
                t
                for t in freeze_frame
                # Cartesian coordinates
                if not t["teammate"] and t["position"]["name"] == "Goalkeeper"
            ),
            None,
        )
        if goalkeeper is None:
            # The goalkeeper is not included in the freeze_frame
            continue

        # Cartesian coordinates
        goalkeeper_x, goalkeeper_y = _sb_to_spadl(
            goalkeeper["location"][0], goalkeeper["location"][1]
        )

        # Polar coordinates
        dx_gk = spadlcfg.field_length - goalkeeper_x
        dy_gk = spadlcfg.field_width / 2 - goalkeeper_y
        goalkeeper_dist_to_goal = math.sqrt(dx_gk**2 + dy_gk**2)
        goalkeeper_angle_to_goal = math.atan2(dy_gk, dx_gk)  # if dx_gk > 0 else 0

        ball_x, ball_y = _sb_to_spadl(shot["location"][0], shot["location"][1])
        dx_kb = goalkeeper_x - ball_x
        dy_kb = goalkeeper_y - ball_y
        goalkeeper_dist_to_ball = math.sqrt(dx_kb**2 + dy_kb**2)

        output.loc[idx, "shot_goalkeeper_x"] = goalkeeper_x
        output.loc[idx, "shot_goalkeeper_y"] = goalkeeper_y
        output.loc[idx, "shot_goalkeeper_dist_to_ball"] = goalkeeper_dist_to_ball
        output.loc[idx, "shot_goalkeeper_dist_to_goal"] = goalkeeper_dist_to_goal
        output.loc[idx, "shot_goalkeeper_angle_to_goal"] = goalkeeper_angle_to_goal
    return output


def create_statsbomb_shot_nb_defenders_in_radius_feature(
    radius: int = 100,
) -> Callable[[DataFrame[StatsBombEventSchema], Mask], Features]:
    """Create a feature generator that computes the number of defenders within a certain radius of the ball on a shot.

    Parameters
    ----------
    radius : float, optional
        The radius (in centimeters) within which to count defenders.
    """

    def feature_func(events: DataFrame[StatsBombEventSchema], mask: Mask) -> Features:
        """Get the number of defenders within a certain radius of the ball.

        Parameters
        ----------
        events : DataFrame[StatsBombEventSchema]
            The StatsBomb events of a game.
        mask : Mask
            A boolean mask to select the shots for which attributes should be
            computed.

        Returns
        -------
        pd.DataFrame
            A dataframe with a column containing the number of defenders within
            the specified radius of the ball ('shot_nb_defenders_in_<r>cm_radius').
        """
        # Filter rows where 'shot' exists in 'extra'
        mask_shots = _mask_shots(events)

        # Create the output DataFrame
        feature_name = f"shot_nb_defenders_in_{radius}cm_radius"
        output = pd.DataFrame(
            data={
                feature_name: pd.Series(dtype="int", index=events.loc[mask].index),
            },
            index=events.loc[mask].index,
        )
        for idx, shot in events.loc[mask & mask_shots].iterrows():
            if "shot" not in shot.extra or "freeze_frame" not in shot.extra["shot"]:
                # No freeze frame data available for this shot
                continue
            freeze_frame = shot.extra["shot"]["freeze_frame"]
            defenders = [t for t in freeze_frame if not t["teammate"]]
            within_radius = 0
            for defender in defenders:
                if defender["teammate"] or defender["position"]["name"] == "Goalkeeper":
                    continue
                defender_x, defender_y = _sb_to_spadl(
                    defender["location"][0], defender["location"][1]
                )
                ball_x, ball_y = _sb_to_spadl(shot["location"][0], shot["location"][1])
                dist = math.sqrt((ball_x - defender_x) ** 2 + (ball_y - defender_y) ** 2)
                if dist * 100 <= radius:
                    within_radius += 1
            output.loc[idx, feature_name] = within_radius

        return output

    return feature_func


statsbomb_shot_defenders_in_radius = feature_generator(
    "events", features=["shot_nb_defenders_in_100cm_radius"]
)(create_statsbomb_shot_nb_defenders_in_radius_feature(radius=100))


def create_statsbomb_shot_pressure_context_feature(
    smoothed: bool = False,
    sigma: float = 1.0,
) -> Callable[[DataFrame[StatsBombEventSchema], Mask], Features]:
    """Create a feature generator that computes features describing the position of the defending players on a shot.

    Parameters
    ----------
    smoothed : bool, optional
        Whether to compute a smoothed version of the features by
        representing defenders as 2D bell-shaped surfaces (2D Gaussian
        distributions) instead of fixed-radius circles [1].
    sigma : float, optional
        Standard deviation for the Gaussian representation of defenders
        (meters) if smoothed is True.

    Returns
    -------
    Callable
        A feature generator.

    References
    ----------
    .. [1] Dinesh Vatvani. "Upgrading Expected Goals". StatsBomb, May 15, 2022.
           https://blogarchive.statsbomb.com/articles/soccer/upgrading-expected-goals/
    """

    def feature_func(events: DataFrame[StatsBombEventSchema], mask: Mask) -> Features:
        """Get features describing the position of the defending players.

        The following features are computed:
            - shot_dist_to_defender: The distance to the closest defender.
            - shot_nb_defenders_in_shot_triangle: The number of defenders in the visible
              angle to the goal.
            - shot_goalkeeper_in_shot_triangle: Whether the goalkeeper is in the visible
              angle to the goal.
            - shot_nb_defenders_behind_ball: The number of defenders behind the ball.
            - shot_one_on_one_derived: Whether the shot was a 1 versus 1 situation. We define
              a 1v1 as a shot for which only the goalkeeper is inside the triangle
              formed by joining the shot location, the right post, and the left
              post. Also, the striker must be higher up the pitch than any other
              defending player apart from the opposition goalkeeper and the shot
              is made with the player's foot. [1]_

        Parameters
        ----------
        events : DataFrame[StatsBombEventSchema]
            The StatsBomb events of a game.
        mask : Mask
            A boolean mask to select the shots for which attributes should be
            computed.

        Returns
        -------
        pd.DataFrame

        References
        ----------
        .. [1] Wear, Matthew, et al. "Learning from the Pros: Extracting
           Professional Goalkeeper Technique from Broadcast Footage." arXiv preprint
           arXiv:2202.12259 (2022).
        """
        # Filter rows where 'shot' exists in 'extra'
        mask_shots = _mask_shots(events)

        # Create the output DataFrame
        output = pd.DataFrame(
            data={
                "shot_dist_to_defender": pd.Series(dtype="float", index=events.loc[mask].index),
                "shot_nb_defenders_in_shot_triangle": pd.Series(
                    dtype="float" if smoothed else "int", index=events.loc[mask].index
                ),
                "shot_goalkeeper_in_shot_triangle": pd.Series(
                    dtype="float" if smoothed else "bool", index=events.loc[mask].index
                ),
                "shot_nb_defenders_behind_ball": pd.Series(
                    dtype="float" if smoothed else "int", index=events.loc[mask].index
                ),
                "shot_one_on_one_derived": pd.Series(
                    dtype="float" if smoothed else "bool", index=events.loc[mask].index
                ),
            },
            index=events.loc[mask].index,
        )

        left_post = (
            spadlcfg.field_length,
            spadlcfg.field_width / 2 - _spadl_cfg["goal_width"] / 2,
        )
        right_post = (
            spadlcfg.field_length,
            spadlcfg.field_width / 2 + _spadl_cfg["goal_width"] / 2,
        )
        for idx, shot in events.loc[mask & mask_shots].iterrows():
            if "shot" not in shot.extra or "freeze_frame" not in shot.extra["shot"]:
                # No freeze frame data available for this shot
                continue
            defenders = [t for t in shot.extra["shot"]["freeze_frame"] if not t["teammate"]]
            distances = []
            goalkeeper_in_shot_triangle = None
            defenders_in_shot_triangle = 0
            defenders_behind_ball = 0
            for defender in defenders:
                defender_x, defender_y = _sb_to_spadl(
                    defender["location"][0], defender["location"][1]
                )
                ball_x, ball_y = _sb_to_spadl(shot["location"][0], shot["location"][1])
                in_shot_triangle = (
                    is_inside_triangle_smoothed(
                        (defender_x, defender_y),
                        ((sigma, 0), (0, sigma)),
                        (left_post, (ball_x, ball_y), right_post),
                    )
                    if smoothed
                    else is_inside_triangle(
                        (defender_x, defender_y),
                        (left_post, (ball_x, ball_y), right_post),
                    )
                )
                behind_ball = (
                    gaussian_mass_right_of_x(
                        (defender_x, defender_y), ((sigma, 0), (0, sigma)), ball_x
                    )
                    if smoothed
                    else right_of_x((defender_x, defender_y), ball_x)
                )
                if defender["position"]["name"] == "Goalkeeper":
                    goalkeeper_in_shot_triangle = in_shot_triangle
                else:
                    distances.append(
                        math.sqrt((ball_x - defender_x) ** 2 + (ball_y - defender_y) ** 2)
                    )
                    defenders_in_shot_triangle += in_shot_triangle
                    defenders_behind_ball += behind_ball if smoothed else int(behind_ball)
            output.loc[idx, "shot_dist_to_defender"] = min(distances, default=float("inf"))
            output.loc[idx, "shot_nb_defenders_in_shot_triangle"] = defenders_in_shot_triangle
            output.loc[idx, "shot_goalkeeper_in_shot_triangle"] = goalkeeper_in_shot_triangle
            output.loc[idx, "shot_nb_defenders_behind_ball"] = defenders_behind_ball
            output.loc[idx, "shot_one_on_one_derived"] = (
                math.exp(-defenders_in_shot_triangle)
                * int(shot.extra["shot"]["body_part"]["name"] in ["Left Foot", "Right Foot"])
                if smoothed
                else bool(
                    defenders_behind_ball == 0
                    and defenders_in_shot_triangle == 0
                    and shot.extra["shot"]["body_part"]["name"] in ["Left Foot", "Right Foot"]
                )
            )

        return output

    return feature_func


statsbomb_shot_pressure_context = feature_generator(
    "events",
    features=[
        "shot_dist_to_defender",
        "shot_nb_defenders_in_shot_triangle",
        "shot_goalkeeper_in_shot_triangle",
        "shot_nb_defenders_behind_ball",
        "shot_one_on_one_derived",
    ],
)(create_statsbomb_shot_pressure_context_feature(smoothed=False))

statsbomb_shot_pressure_context_smoothed = feature_generator(
    "events",
    features=[
        "shot_dist_to_defender",
        "shot_nb_defenders_in_shot_triangle",
        "shot_goalkeeper_in_shot_triangle",
        "shot_nb_defenders_behind_ball",
        "shot_one_on_one_derived",
    ],
)(create_statsbomb_shot_pressure_context_feature(smoothed=True))


@feature_generator(
    "events",
    features=[
        "shot_end_x_assist",
        "shot_end_y_assist",
        "shot_carry_dist",
        "shot_type_assist",
        "shot_height_assist",
    ],
)
def statsbomb_shot_assist(events: DataFrame[StatsBombEventSchema], mask: Mask) -> Features:  # noqa: C901
    """Get features describing the assist.

    The following features are computed:
        - shot_end_x_assist: The assisting pass' x-coordinate
        - shot_end_y_assist: The assisting pass' y-coordinate
        - shot_carry_dist: The distance between the end location of the assisting
          pass and the location of the shot.
        - shot_type_assist: The assist type, which is one of 'standard_pass',
          'free_kick', 'corner', 'throw_in', 'cross', 'cut_back' or 'through_ball'.
        - shot_height_assist: The peak height of the assisting pass, which is one of
          'ground', 'low' (under shoulder level) or 'high' (above shoulder
          level).

    Parameters
    ----------
    events : DataFrame[StatsBombEventSchema]
        The StatsBomb events of a game.
    mask : Mask
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    pd.DataFrame

    References
    ----------
    .. [1] Wear, Matthew, et al. "Learning from the Pros: Extracting
       Professional Goalkeeper Technique from Broadcast Footage." arXiv preprint
       arXiv:2202.12259 (2022).
    """
    # Filter rows where 'shot' exists in 'extra'
    mask_shots = _mask_shots(events)

    # Create the output DataFrame
    index = events.loc[mask].index
    output = pd.DataFrame(
        {
            "shot_end_x_assist": pd.Series(dtype="float", index=index),
            "shot_end_y_assist": pd.Series(dtype="float", index=index),
            "shot_carry_dist": pd.Series(dtype="float", index=index),
            "shot_type_assist": pd.Categorical(
                pd.Series(dtype="category", index=index),
                categories=[
                    "standard_pass",
                    "free_kick",
                    "corner",
                    "throw_in",
                    "cross",
                    "cut_back",
                    "through_ball",
                ],
                ordered=False,
            ),
            "shot_height_assist": pd.Categorical(
                pd.Series(dtype="category", index=index),
                categories=["ground", "low", "high"],
                ordered=True,
            ),
        },
        index=index,
    )

    indexed_events = events.set_index("event_id")
    for idx, shot in events.loc[mask & mask_shots].iterrows():
        if "key_pass_id" not in shot.extra["shot"]:
            # No assist for this shot
            continue
        assist = indexed_events.loc[shot.extra["shot"]["key_pass_id"]]
        assist_x, assist_y = _sb_to_spadl(
            assist.extra["pass"]["end_location"][0], assist.extra["pass"]["end_location"][1]
        )
        shot_x, shot_y = _sb_to_spadl(shot["location"][0], shot["location"][1])

        assist_type = "standard_pass"
        assist_height = "ground"
        if "pass" in assist.extra:
            # assist type
            if "cross" in assist.extra["pass"]:
                assist_type = "cross"
            elif "cut_back" in assist.extra["pass"]:
                assist_type = "cut_back"
            elif "technique" in assist.extra["pass"]:
                if assist.extra["pass"]["technique"]["name"] == "Through Ball":
                    assist_type = "through_ball"
            # special pass type
            if "type" in assist.extra["pass"]:
                if assist.extra["pass"]["type"]["name"] == "Free Kick":
                    assist_type = "free_kick"
                elif assist.extra["pass"]["type"]["name"] == "Corner":
                    assist_type = "corner"
                elif assist.extra["pass"]["type"]["name"] == "Throw-in":
                    assist_type = "throw_in"

            # assist height
            if "height" in assist.extra["pass"]:
                m = {
                    "Ground Pass": "ground",
                    "Low Pass": "low",
                    "High Pass": "high",
                }
                assist_height = m[assist.extra["pass"]["height"]["name"]]

        output.loc[idx, "shot_end_x_assist"] = assist_x
        output.loc[idx, "shot_end_y_assist"] = assist_y
        output.loc[idx, "shot_carry_dist"] = math.sqrt(
            (shot_x - assist_x) ** 2 + (shot_y - assist_y) ** 2
        )
        output.loc[idx, "shot_type_assist"] = assist_type
        output.loc[idx, "shot_height_assist"] = assist_height

    return output


@feature_generator("events", features=["shot_impact_height"])
def statsbomb_shot_impact_height(events: DataFrame[StatsBombEventSchema], mask: Mask) -> Features:
    """Get the height of the ball when the shot was taken.

    This is derived from the z-coordinate of the shot location. It is only
    available in recent data versions.

    Parameters
    ----------
    events : DataFrame[StatsBombEventSchema]
        The StatsBomb events of a game.
    mask : Mask
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    pd.DataFrame

    See Also
    --------
    :func:`statsbomb_shot_impact_height_cat`: categorical version that uses
    bodypart and technique as a proxy for the height of the ball.
    """
    # Filter rows where 'shot' exists in 'extra'
    mask_shots = _mask_shots(events)

    # Create the output DataFrame
    index = events.loc[mask].index
    output = pd.DataFrame(
        {"shot_impact_height": pd.Series(dtype="float", index=index)},
        index=index,
    )
    for idx, shot in events.loc[mask & mask_shots].iterrows():
        if len(shot.location) == 3:
            start_x, start_y, start_z = shot.location
        else:
            # Not a valid location / no shot impact height available
            continue

        output.loc[idx, "shot_impact_height"] = start_z

    return output


@feature_generator("events", features=["shot_impact_height_category"])
def statsbomb_shot_impact_height_cat(
    events: DataFrame[StatsBombEventSchema], mask: Mask
) -> Features:
    """Get the height of the ball when the shot was taken.

    This is derived from the bodypart and technique that was used to take the
    shot. Possible values are 'ground', 'low' (below shoulder level) and
    'high' (above shoulder level).

    Parameters
    ----------
    events : DataFrame[StatsBombEventSchema]
        The StatsBomb events of a game.
    mask : Mask
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    pd.DataFrame

    See Also
    --------
    :func:`statsbomb_shot_impact_height`: exact version that uses the z-coordinate
    of the shot if available.
    """
    # Filter rows where 'shot' exists in 'extra'
    mask_shots = _mask_shots(events)

    # Create the output DataFrame
    index = events.loc[mask].index
    output = pd.DataFrame(
        {
            "shot_impact_height": pd.Categorical(
                pd.Series(dtype="category", index=index),
                categories=["ground", "low", "high"],
                ordered=True,
            )
        },
        index=index,
    )

    # The height of the ball when the ball is touched is not included,
    # but we can use body part and technique as a proxy for this
    for idx, shot in events.loc[mask & mask_shots].iterrows():
        if "shot" not in shot.extra or "technique" not in shot.extra["shot"]:
            # No freeze frame data available for this shot
            continue
        height = "ground"
        if shot.extra["shot"]["body_part"]["name"] == "Head":
            if shot.extra["shot"]["technique"]["name"] == "Diving Header":
                height = "low"
            else:
                height = "high"
        elif shot.extra["shot"]["body_part"]["name"] == "Other":
            height = "low"
        elif shot.extra["shot"]["technique"]["name"] == "Half Volley":
            height = "low"
        elif shot.extra["shot"]["technique"]["name"] == "Volley":
            height = "low"
        elif shot.extra["shot"]["technique"]["name"] == "Overhead Kick":
            height = "high"
        output.loc[idx, "shot_impact_height"] = height

    return output


@feature_generator("events", features=["shot_technique"])
def statsbomb_shot_technique(events: DataFrame[StatsBombEventSchema], mask: Mask) -> Features:
    """Get the technique used to take the shot.

    Possible values are 'normal', 'half_volley', 'volley', 'overhead_kick',
    'diving_header', "lob", "backheel" and 'other'.

    Parameters
    ----------
    events : DataFrame[StatsBombEventSchema]
        The StatsBomb events of a game.
    mask : Mask
        A boolean mask to select the shots for which attributes should be
        computed.

    Returns
    -------
    pd.DataFrame
    """
    # Filter rows where 'shot' exists in 'extra'
    mask_shots = _mask_shots(events)
    # Create the output DataFrame
    index = events.loc[mask].index
    output = pd.DataFrame(
        {
            "shot_technique": pd.Categorical(
                pd.Series(dtype="category", index=index),
                categories=[
                    "normal",
                    "half_volley",
                    "volley",
                    "overhead_kick",
                    "diving_header",
                    "lob",
                    "backheel",
                    "other",
                ],
                ordered=False,
            )
        },
        index=index,
    )
    for idx, shot in events.loc[mask & mask_shots].iterrows():
        if "shot" not in shot.extra or "technique" not in shot.extra["shot"]:
            # No freeze frame data available for this shot
            continue
        technique = shot.extra["shot"]["technique"]["name"]
        if technique == "Normal":
            technique = "normal"
        elif technique == "Half Volley":
            technique = "half_volley"
        elif technique == "Volley":
            technique = "volley"
        elif technique == "Overhead Kick":
            technique = "overhead_kick"
        elif technique == "Diving Header":
            technique = "diving_header"
        elif technique == "Lob":
            technique = "lob"
        elif technique == "Backheel":
            technique = "backheel"
        else:
            technique = "other"
        output.loc[idx, "shot_technique"] = technique
    return output
