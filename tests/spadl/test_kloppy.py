import os
from typing import NamedTuple

import pandas as pd
import pytest
from kloppy import opta, statsbomb, wyscout
from kloppy.domain import Orientation
from pandas.testing import assert_frame_equal
from socceraction.data.opta import OptaLoader
from socceraction.data.statsbomb import StatsBombLoader
from socceraction.data.wyscout import PublicWyscoutLoader, WyscoutLoader
from socceraction.spadl import config as spadl
from socceraction.spadl import kloppy as kl
from socceraction.spadl import opta as spadl_opta
from socceraction.spadl import statsbomb as sb
from socceraction.spadl import wyscout as spadl_wyscout

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)


class Dataset(NamedTuple):
    kloppy: pd.DataFrame
    socceraction: pd.DataFrame


@pytest.fixture(scope="session")
def statsbomb_actions() -> Dataset:
    data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "datasets", "statsbomb", "raw")
    kloppy_dataset = statsbomb.load(
        event_data=os.path.join(data_dir, "events", "7584.json"),
        lineup_data=os.path.join(data_dir, "lineups", "7584.json"),
    )
    df_actions_kl = kl.convert_to_actions(kloppy_dataset, game_id=7584)
    SBL = StatsBombLoader(root=data_dir, getter="local")
    df_actions_sa = sb.convert_to_actions(SBL.events(7584), 782)

    return Dataset(df_actions_kl, df_actions_sa)


@pytest.mark.parametrize("actiontype", spadl.actiontypes)
def test_kloppy_to_actions_statsbomb(statsbomb_actions: Dataset, actiontype: str) -> None:
    # columns to compare
    cols = [
        "game_id",
        "original_event_id",
        "period_id",
        "time_seconds",
        "team_id",
        "player_id",
        "start_x",
        "start_y",
        "end_x",
        "end_y",
        "type_id",
        "result_id",
        "bodypart_id",
        # 'action_id',
    ]
    # load statsbomb data using socceraction
    sel_actions_sa = statsbomb_actions.socceraction.loc[
        (statsbomb_actions.socceraction.type_id == spadl.actiontypes.index(actiontype)),
        cols,
    ]
    # load statsbomb data using kloppy
    sel_actions_kl = statsbomb_actions.kloppy.loc[
        (statsbomb_actions.kloppy.type_id == spadl.actiontypes.index(actiontype)),
        cols,
    ].replace({"original_event_id": {"interception-": ""}}, regex=True)
    # FIXME
    sel_actions_sa["team_id"] = sel_actions_sa["team_id"].astype(str)
    sel_actions_sa["player_id"] = sel_actions_sa["player_id"].astype("Int64").astype(str)
    if actiontype in ["keeper_save", "keeper_punch"]:
        sel_actions_sa["result_id"] = spadl.results.index("success")
    #
    print(
        "These events should not be included",
        set(sel_actions_kl.original_event_id) - set(sel_actions_sa.original_event_id),
    )
    print(
        "These events are missing",
        set(sel_actions_sa.original_event_id) - set(sel_actions_kl.original_event_id),
    )
    # compare the two datasets
    assert_frame_equal(
        sel_actions_kl.set_index("original_event_id"),
        sel_actions_sa.set_index("original_event_id"),
    )


@pytest.fixture(scope="session")
def opta_actions() -> Dataset:
    data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "datasets", "opta")
    kloppy_dataset = opta.load(
        f7_data=os.path.join(data_dir, "f7-23-2018-1009316-matchresults.xml"),
        f24_data=os.path.join(data_dir, "f24-23-2018-1009316-eventdetails.xml"),
    )
    df_actions_kl = kl.convert_to_actions(kloppy_dataset, game_id=1009316)
    loader = OptaLoader(
        root=data_dir,
        parser="xml",
        feeds={
            "f7": "f7-{competition_id}-{season_id}-{game_id}-matchresults.xml",
            "f24": "f24-{competition_id}-{season_id}-{game_id}-eventdetails.xml",
        },
    )
    df_actions_sa = spadl_opta.convert_to_actions(loader.events(1009316), 174)

    return Dataset(df_actions_kl, df_actions_sa)


# def test_dummy_opta() -> None:
#     data_dir = os.path.join(os.path.dirname(__file__), os.pardir, 'datasets', 'opta')
#     kloppy_dataset = opta.load(
#         f7_data=os.path.join(data_dir, "f7-23-2018-1009316-matchresults.xml"),
#         f24_data=os.path.join(data_dir, "f24-23-2018-1009316-eventdetails.xml"),
#     ).transform(
#         to_orientation=Orientation.FIXED_HOME_AWAY,  # FIXME
#         to_coordinate_system=kl._SoccerActionCoordinateSystem(normalized=False),
#     )
#
#     event = kloppy_dataset.get_event_by_id("1592827425")
#     print(event)
#     loader = OptaLoader(
#         root=data_dir,
#         parser='xml',
#         feeds={
#             'f7': 'f7-{competition_id}-{season_id}-{game_id}-matchresults.xml',
#             'f24': 'f24-{competition_id}-{season_id}-{game_id}-eventdetails.xml',
#         },
#     )
#     df = loader.events(1009316)
#     print(df.loc[df.event_id == 1592827425])
#
#     assert False


@pytest.mark.skip(reason="not yet supported")
@pytest.mark.parametrize("actiontype", spadl.actiontypes)
def test_kloppy_to_actions_opta(opta_actions: Dataset, actiontype: str) -> None:
    # columns to compare
    cols = [
        "game_id",
        "original_event_id",
        "period_id",
        # 'time_seconds', # FIXME
        "team_id",
        "player_id",
        "start_x",
        "start_y",
        "end_x",
        "end_y",
        "type_id",
        "result_id",
        "bodypart_id",
        # 'action_id',
    ]
    # load statsbomb data using socceraction
    sel_actions_sa = opta_actions.socceraction.loc[
        (opta_actions.socceraction.type_id == spadl.actiontypes.index(actiontype)),
        cols,
    ]
    # load statsbomb data using kloppy
    sel_actions_kl = opta_actions.kloppy.loc[
        (opta_actions.kloppy.type_id == spadl.actiontypes.index(actiontype)),
        cols,
    ]
    # FIXME
    sel_actions_kl["team_id"] = sel_actions_kl["team_id"].astype(int)
    sel_actions_kl["player_id"] = sel_actions_kl["player_id"].astype(float)
    # sel_actions_kl["original_event_id"] = sel_actions_kl["original_event_id"].astype(float)
    sel_actions_sa["original_event_id"] = sel_actions_sa["original_event_id"].astype(str)
    #
    print(
        "These events should not be included",
        set(sel_actions_kl.original_event_id) - set(sel_actions_sa.original_event_id),
    )
    print(
        "These events are missing",
        set(sel_actions_sa.original_event_id) - set(sel_actions_kl.original_event_id),
    )
    # compare the two datasets
    assert_frame_equal(
        sel_actions_kl.set_index("original_event_id"),
        sel_actions_sa.set_index("original_event_id"),
    )


@pytest.fixture(scope="session")
def wyscout_actions() -> Dataset:
    data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "datasets", "wyscout_api")
    kloppy_dataset = wyscout.load(
        event_data=os.path.join(data_dir, "events_2852835.json"),
    )
    df_actions_kl = kl.convert_to_actions(kloppy_dataset, game_id=2852835)
    WSL = WyscoutLoader(
        root=data_dir,
        getter="local",
        feeds={
            "competitions": "competitions.json",
            "seasons": "seasons_{competition_id}.json",
            # "games": "matches_{season_id}.json",
            "events": "events_{game_id}.json",
        },
    )
    df_actions_sa = spadl_wyscout.convert_to_actions(WSL.events(2852835), 3166)

    return Dataset(df_actions_kl, df_actions_sa)


@pytest.fixture(scope="session")
def public_wyscout_actions() -> tuple[pd.DataFrame, pd.DataFrame]:
    data_dir = os.path.join(
        os.path.dirname(__file__), os.pardir, "datasets", "wyscout_public", "raw"
    )
    kloppy_dataset = wyscout.load_open_data(match_id="2058007")
    kloppy_dataset.metadata.orientation = Orientation.ACTION_EXECUTING_TEAM
    df_actions_kl = kl.convert_to_actions(kloppy_dataset, game_id=2058007)
    WSL = PublicWyscoutLoader(root=data_dir, download=False)
    df_actions_sa = spadl_wyscout.convert_to_actions(WSL.events(2058007), 5629)

    return Dataset(df_actions_kl, df_actions_sa)


# def test_dummy_wyscout() -> None:
#     kloppy_dataset = wyscout.load_open_data(match_id="2058007").transform(
#         to_orientation=Orientation.FIXED_HOME_AWAY,  # FIXME
#         to_coordinate_system=kl._SoccerActionCoordinateSystem(normalized=False),
#     )
#
#     event = kloppy_dataset.get_event_by_id("261445568")
#     print(event)
#     print(event.qualifiers)
#     print(event.coordinates)
#     # print(event.end_coordinates)
#     print(event.raw_event)
#
#     assert False


@pytest.mark.skip(reason="not yet supported")
@pytest.mark.parametrize("actiontype", spadl.actiontypes)
def test_kloppy_to_actions_wyscout(public_wyscout_actions: Dataset, actiontype: str) -> None:
    # columns to compare
    cols = [
        "game_id",
        "original_event_id",
        "period_id",
        "time_seconds",
        "team_id",
        "player_id",
        "start_x",
        "start_y",
        "end_x",
        "end_y",
        "type_id",
        "result_id",
        "bodypart_id",
        # 'action_id',
    ]
    # load statsbomb data using socceraction
    sel_actions_sa = public_wyscout_actions.socceraction.loc[
        (public_wyscout_actions.socceraction.type_id == spadl.actiontypes.index(actiontype)),
        cols,
    ]
    # load statsbomb data using kloppy
    sel_actions_kl = public_wyscout_actions.kloppy.loc[
        (public_wyscout_actions.kloppy.type_id == spadl.actiontypes.index(actiontype)),
        cols,
    ].replace({"original_event_id": {"interception-": ""}}, regex=True)

    # FIXME
    sel_actions_kl["team_id"] = sel_actions_kl["team_id"].astype(int)
    sel_actions_kl["player_id"] = sel_actions_kl["player_id"].astype(int)
    sel_actions_sa["original_event_id"] = sel_actions_sa["original_event_id"].astype(str)
    #
    print(
        "These events should not be included",
        set(sel_actions_kl.original_event_id) - set(sel_actions_sa.original_event_id),
    )
    print(
        "These events are missing",
        set(sel_actions_sa.original_event_id) - set(sel_actions_kl.original_event_id),
    )
    # compare the two datasets
    assert_frame_equal(
        sel_actions_kl.set_index("original_event_id"),
        sel_actions_sa.set_index("original_event_id"),
    )
