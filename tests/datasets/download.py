"""Script for downloading test data."""
import argparse
import logging
import os
import shutil
import ssl
import warnings
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen, urlretrieve
from zipfile import ZipFile, is_zipfile

import pandas as pd

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

import socceraction.atomic.spadl as atomicspadl
import socceraction.spadl as spadl
import socceraction.spadl.statsbomb as statsbomb
import socceraction.spadl.wyscout as wyscout
from socceraction.data.statsbomb import StatsBombLoader
from socceraction.data.wyscout import PublicWyscoutLoader

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
warnings.filterwarnings(
    action="ignore", message="credentials were not supplied. open data access only"
)


# optional: if you get a SSL CERTIFICATE_VERIFY_FAILED exception
ssl._create_default_https_context = ssl._create_unverified_context

_data_dir = os.path.dirname(__file__)


def download_statsbomb_data() -> None:
    """Download and extract the StatsBomb open data repository."""
    logging.info("Downloading StatsBomb data")
    dataset_url = "https://github.com/statsbomb/open-data/archive/master.zip"

    tmp_datafolder = os.path.join(_data_dir, "statsbomb", "tmp")
    raw_datafolder = os.path.join(_data_dir, "statsbomb", "raw")
    for datafolder in [tmp_datafolder, raw_datafolder]:
        if not os.path.exists(datafolder):
            os.makedirs(datafolder, exist_ok=True)
    statsbombzip = os.path.join(tmp_datafolder, "statsbomb-open-data.zip")

    with urlopen(dataset_url) as dl_file:
        with open(statsbombzip, "wb") as out_file:
            out_file.write(dl_file.read())

    with ZipFile(statsbombzip, "r") as zipObj:
        zipObj.extractall(tmp_datafolder)

    shutil.rmtree(raw_datafolder)
    Path(f"{tmp_datafolder}/open-data-master/data").rename(raw_datafolder)
    shutil.rmtree(tmp_datafolder)
    logging.info("Done! Data was saved to %s", raw_datafolder)


def convert_statsbomb_data() -> None:
    """Convert StatsBomb data to SPADL."""
    logging.info("Converting StatsBomb data")
    seasons = {
        3: "2018",
    }
    leagues = {
        "FIFA World Cup": "WorldCup",
    }
    spadl_datafolder = os.path.join(_data_dir, "statsbomb")

    free_open_data_remote = "https://raw.githubusercontent.com/statsbomb/open-data/master/data/"

    SBL = StatsBombLoader(root=free_open_data_remote, getter="remote")

    # View all available competitions
    df_competitions = SBL.competitions()
    selected_competitions = df_competitions.competition_name.isin(leagues.keys())
    df_selected_competitions = df_competitions.loc[selected_competitions]

    for competition in df_selected_competitions.itertuples():
        # Get games from all selected competition
        games = SBL.games(competition.competition_id, competition.season_id)

        if tqdm is not None:
            games_verbose = tqdm(list(games.itertuples()), desc="Loading match data")
        else:
            games_verbose = games.itertuples()
        teams, players = [], []

        competition_id = leagues[competition.competition_name]
        season_id = seasons[competition.season_id]
        spadl_h5 = os.path.join(spadl_datafolder, f"spadl-{competition_id}-{season_id}.h5")
        with pd.HDFStore(spadl_h5) as spadlstore:
            spadlstore.put("actiontypes", spadl.actiontypes_df(), format="table")
            spadlstore.put("results", spadl.results_df(), format="table")
            spadlstore.put("bodyparts", spadl.bodyparts_df(), format="table")

            for game in games_verbose:
                # load data
                teams.append(SBL.teams(game.game_id))
                players.append(SBL.players(game.game_id))
                events = SBL.events(game.game_id)

                # convert data
                spadlstore.put(
                    f"actions/game_{game.game_id}",
                    statsbomb.convert_to_actions(events, game.home_team_id),
                    format="table",
                )

            games.season_id = season_id
            games.competition_id = competition_id
            spadlstore.put("games", games)
            spadlstore.put(
                "teams",
                pd.concat(teams).drop_duplicates("team_id").reset_index(drop=True),
            )
            spadlstore.put(
                "players",
                pd.concat(players).drop_duplicates("player_id").reset_index(drop=True),
            )
    logging.info("Done! Data was saved to %s", spadl_datafolder)


def download_wyscout_data() -> None:
    """Download and extract the Wyscout public dataset."""
    logging.info("Downloading Wyscout data")
    # https://figshare.com/collections/Soccer_match_event_dataset/4415000/5
    dataset_urls = dict(
        competitions="https://ndownloader.figshare.com/files/15073685",
        teams="https://ndownloader.figshare.com/files/15073697",
        players="https://ndownloader.figshare.com/files/15073721",
        games="https://ndownloader.figshare.com/files/14464622",
        events="https://ndownloader.figshare.com/files/14464685",
    )

    raw_datafolder = os.path.join(_data_dir, "wyscout_public", "raw")
    if not os.path.exists(raw_datafolder):
        os.makedirs(raw_datafolder, exist_ok=True)

    # download and unzip Wyscout open data
    for url in dataset_urls.values():
        url_obj = urlopen(url).geturl()
        path = Path(urlparse(url_obj).path)
        file_name = os.path.join(raw_datafolder, path.name)
        file_local, _ = urlretrieve(url_obj, file_name)
        if is_zipfile(file_local):
            with ZipFile(file_local) as zip_file:
                zip_file.extractall(raw_datafolder)
    logging.info("Done! Data was saved to %s", raw_datafolder)


def convert_wyscout_data() -> None:
    """Convert Wyscout data to SPADL."""
    logging.info("Converting Wyscout data")
    seasons = {
        10078: "2018",
    }
    leagues = {
        28: "WorldCup",
    }

    raw_datafolder = os.path.join(_data_dir, "wyscout_public", "raw")
    spadl_datafolder = os.path.join(_data_dir, "wyscout_public")

    WYL = PublicWyscoutLoader(root=raw_datafolder)

    # View all available competitions
    df_competitions = WYL.competitions()
    selected_competitions = df_competitions.competition_id.isin(leagues.keys())
    df_selected_competitions = df_competitions.loc[selected_competitions]

    for competition in df_selected_competitions.itertuples():
        # Get games from all selected competition
        games = WYL.games(competition.competition_id, competition.season_id)

        if tqdm is not None:
            games_verbose = tqdm(list(games.itertuples()), desc="Loading match data")
        else:
            games_verbose = games.itertuples()
        teams, players = [], []

        competition_id = leagues[competition.competition_id]
        season_id = seasons[competition.season_id]
        spadl_h5 = os.path.join(spadl_datafolder, f"spadl-{competition_id}-{season_id}.h5")
        with pd.HDFStore(spadl_h5) as spadlstore:
            spadlstore.put("actiontypes", spadl.actiontypes_df(), format="table")
            spadlstore.put("results", spadl.results_df(), format="table")
            spadlstore.put("bodyparts", spadl.bodyparts_df(), format="table")

            for game in games_verbose:
                # load data
                teams.append(WYL.teams(game.game_id))
                players.append(WYL.players(game.game_id))
                events = WYL.events(game.game_id)

                # convert data
                spadlstore.put(
                    f"actions/game_{game.game_id}",
                    wyscout.convert_to_actions(events, game.home_team_id),
                    # format='table',
                )

            games.season_id = season_id
            games.competition_id = competition_id
            spadlstore.put("games", games)
            spadlstore.put(
                "teams",
                pd.concat(teams).drop_duplicates("team_id").reset_index(drop=True),
            )
            spadlstore.put(
                "players",
                pd.concat(players).drop_duplicates("player_id").reset_index(drop=True),
            )
    logging.info("Done! Data was saved to %s", spadl_datafolder)


def create_spadl(game_id: int, home_team_id: int) -> None:
    """Create SPADL actions from StatsBomb data for a given game."""
    logging.info("Creating SPADL data")
    spadl_datafolder = os.path.join(_data_dir, "spadl")
    if not os.path.exists(spadl_datafolder):
        os.makedirs(spadl_datafolder, exist_ok=True)

    # load events
    free_open_data_remote = "https://raw.githubusercontent.com/statsbomb/open-data/master/data/"
    SBL = StatsBombLoader(root=free_open_data_remote, getter="remote")
    events = SBL.events(game_id)
    # convert to spadl
    spadl_json = os.path.join(spadl_datafolder, "spadl.json")
    df_actions = statsbomb.convert_to_actions(events, home_team_id)
    df_actions.head(n=200).to_json(spadl_json, orient="records")
    # convert to atomic spadl
    atomic_spadl_json = os.path.join(spadl_datafolder, "atomic_spadl.json")
    df_atomic_actions = atomicspadl.convert_to_atomic(df_actions)
    df_atomic_actions.head(n=200).to_json(atomic_spadl_json, orient="records")
    logging.info("Done! SPADL data was saved to %s and %s", spadl_json, atomic_spadl_json)


if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(level=logging.INFO)

    # Create the parser
    my_parser = argparse.ArgumentParser(
        prog="download",
        usage="%(prog)s [options]",
        formatter_class=argparse.RawTextHelpFormatter,
        description="""Download and prepare the data needed for running the tests.

        Use the options specified below to select specific preprocessing
        steps. When this script is run without any options, all preprocessing
        steps required to run the default test setup will be executed.
        """,
    )

    # Add the arguments
    my_parser.add_argument(
        "--download-statsbomb",
        action="store_true",
        help="Download the public StatsBomb data.",
    )
    my_parser.add_argument(
        "--convert-statsbomb",
        action="store_true",
        help="Convert the public StatsBomb data to SPADL.",
    )
    my_parser.add_argument(
        "--download-wyscout", action="store_true", help="Download the public Wyscout data."
    )
    my_parser.add_argument(
        "--convert-wyscout", action="store_true", help="Convert the public Wyscout data to SPADL."
    )
    my_parser.add_argument(
        "--spadl",
        action="store_true",
        help="Create a JSON file with example SPADL and Atomic-SPADL data.",
    )

    # Execute the parse_args() method
    args = my_parser.parse_args()
    no_options = not any(
        [
            args.download_statsbomb,
            args.convert_statsbomb,
            args.download_wyscout,
            args.convert_wyscout,
            args.spadl,
        ]
    )

    # Run the requested steps
    if args.download_statsbomb or no_options:
        download_statsbomb_data()
    if args.convert_statsbomb:
        convert_statsbomb_data()
    if args.download_wyscout or no_options:
        download_wyscout_data()
    if args.convert_wyscout:
        convert_wyscout_data()
    if args.spadl:
        create_spadl(8657, 777)
