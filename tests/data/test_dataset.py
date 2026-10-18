"""Tests for the socceraction.data.dataset module."""

from collections.abc import Generator
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from pytest import fixture
from socceraction.data import (
    Dataset,
    FSSpecAdapter,
    HDFDataset,
    ParquetDataset,
    PartitionIdentifier,
    SQLDataset,
    StatsBombLoader,
)
from socceraction.data.transforms import ActionsToAtomic, StatsBombEventsToActions, Transform
from sqlalchemy import create_engine


def test_create_sqlite_dataset() -> None:
    """It should add the requested data to the database."""
    engine = create_engine("sqlite://")
    data_loader = StatsBombLoader(getter="remote")
    with SQLDataset(engine) as db:
        db.import_data(
            data_loader, PartitionIdentifier(competition_id=55, season_id=43, game_id=3795107)
        )
        df_competitions = db.read_query("SELECT * FROM competitions")
        assert len(df_competitions) == 1
        df_games = db.read_query("SELECT * FROM competitions")
        assert len(df_games) == 1
        df_teams = db.read_query("SELECT * FROM teams")
        assert len(df_teams) == 2
        df_players = db.read_query("SELECT * FROM player_games")
        assert len(df_players) == 30
        df_events = db.read_query("SELECT * FROM events")
        assert len(df_events) > 0


def test_create_hdf_dataset(tmp_path: Path) -> None:
    """It should add the requested data to the database."""
    db_path = tmp_path / "db.h5"
    data_loader = StatsBombLoader(getter="remote")
    with HDFDataset(db_path, mode="w") as db:
        db.import_data(
            data_loader, PartitionIdentifier(competition_id=55, season_id=43, game_id=3795107)
        )
        df_competitions = db["competitions"]
        assert len(df_competitions) == 1
        df_games = db["games"]
        assert len(df_games) == 1
        df_teams = db["teams"]
        assert len(df_teams) == 2
        df_players = db["player_games"]
        assert len(df_players) == 30
        df_player_games = db["player_games"]
        assert len(df_player_games) == 30


def test_create_parquet_dataset(tmp_path: Path) -> None:
    """It should add the requested data to the Parquet dataset."""
    db_path = tmp_path / "parquet_ds"
    data_loader = StatsBombLoader(getter="remote")
    with ParquetDataset(db_path) as db:
        db.import_data(
            data_loader, PartitionIdentifier(competition_id=55, season_id=43, game_id=3795107)
        )
        df_competitions = db["competitions"]
        assert len(df_competitions) == 1
        df_games = db["games"]
        assert len(df_games) == 1
        df_teams = db["teams"]
        assert len(df_teams) == 2
        df_players = db["player_games"]
        assert len(df_players) == 30
        df_events = db["events/game_3795107"]
        assert len(df_events) > 0


def test_create_parquet_dataset_memory() -> None:
    """It should support in-memory fsspec filesystem."""
    data_loader = StatsBombLoader(getter="remote")
    with ParquetDataset("memory://test_dataset") as db:
        db.import_data(
            data_loader, PartitionIdentifier(competition_id=55, season_id=43, game_id=3795107)
        )
        assert len(db.competitions()) == 1
        assert len(db.events(game_id=3795107)) > 0


@fixture(scope="session")
def sqlitedb() -> Generator[SQLDataset, None, None]:
    """Create a SQL dataset with BEL v ITA at EURO2020."""
    engine = create_engine("sqlite://")
    data_loader = StatsBombLoader(getter="remote")
    with SQLDataset(engine) as db:
        db.import_data(
            data_loader, PartitionIdentifier(competition_id=55, season_id=43, game_id=3795107)
        )
        yield db


@fixture(scope="session")
def hdfdb() -> Generator[HDFDataset, None, None]:
    """Create a HDF dataset with BEL v ITA at EURO2020."""
    data_loader = StatsBombLoader(getter="remote")
    with HDFDataset(":memory:", mode="w", driver="H5FD_CORE", driver_core_backing_store=0) as db:
        db.import_data(
            data_loader, PartitionIdentifier(competition_id=55, season_id=43, game_id=3795107)
        )
        yield db


@fixture(scope="session")
def parquetdb(tmp_path_factory: pytest.TempPathFactory) -> Generator[ParquetDataset, None, None]:
    """Create a Parquet dataset with BEL v ITA at EURO2020."""
    db_path = tmp_path_factory.mktemp("parquet_dataset")
    data_loader = StatsBombLoader(getter="remote")
    with ParquetDataset(db_path) as db:
        db.import_data(
            data_loader, PartitionIdentifier(competition_id=55, season_id=43, game_id=3795107)
        )
        yield db


database_interfaces = [
    pytest.lazy_fixture("sqlitedb"),  # type: ignore[operator]
    pytest.lazy_fixture("hdfdb"),  # type: ignore[operator]
    pytest.lazy_fixture("parquetdb"),  # type: ignore[operator]
]


@pytest.mark.parametrize("db", database_interfaces)
def test_create_dataset_duplicates(db: Dataset) -> None:
    """The database should not contain duplicate data."""
    data_loader = StatsBombLoader(getter="remote")
    db.import_data(
        data_loader, PartitionIdentifier(competition_id=55, season_id=43, game_id=3795107)
    )
    db.import_data(
        data_loader, PartitionIdentifier(competition_id=55, season_id=43, game_id=3795107)
    )
    df_games = db.games()
    assert len(df_games) == 1


@pytest.mark.parametrize("db", database_interfaces)
def test_games(db: Dataset) -> None:
    """It should return the games for a given competition and season."""
    df_games = db.games()
    assert len(df_games) == 1


@pytest.mark.parametrize("db", database_interfaces)
def test_teams(db: Dataset) -> None:
    """It should return the teams in the dataset."""
    df_teams = db.teams()
    assert len(df_teams) == 2


@pytest.mark.parametrize("db", database_interfaces)
def test_players(db: Dataset) -> None:
    """It should return the players for a given game."""
    df_players = db.players()
    assert len(df_players) == 30
    df_players = db.players(game_id=3795107)
    assert len(df_players) == 30
    with pytest.raises(LookupError, match="No game found with ID=0"):
        df_players = db.players(game_id=0)


@pytest.mark.parametrize("db", database_interfaces)
def test_events(db: Dataset) -> None:
    """It should return the events for a given game."""
    df_events = db.events(game_id=3795107)
    assert len(df_events) > 0
    with pytest.raises(LookupError, match="No events found for game with ID=0"):
        db.events(game_id=0)


@pytest.mark.parametrize("db", database_interfaces)
def test_get_home_away_team_id(db: Dataset) -> None:
    """It should return the ID of the home and away team in a given game."""
    home_team_id, away_team_id = db.get_home_away_team_id(game_id=3795107)
    assert home_team_id == 782
    assert away_team_id == 914
    with pytest.raises(LookupError, match="No game found with ID=0"):
        db.get_home_away_team_id(game_id=0)


@pytest.mark.parametrize("db", database_interfaces)
def test_get_player_name(db: Dataset) -> None:
    """It should return the name of the player with a given ID."""
    player_name = db.get_player_name(player_id=3089)
    assert player_name == "Kevin De Bruyne"
    with pytest.raises(LookupError, match="No player found with ID=0"):
        db.get_player_name(player_id=0)


@pytest.mark.parametrize("db", database_interfaces)
def test_search_player(db: Dataset) -> None:
    """It should return a list with matching players."""
    players = db.search_player("Kevin").set_index("player_id")
    assert len(players) == 1
    assert players.at[3089, "player_name"] == "Kevin De Bruyne"


@pytest.mark.parametrize("db", database_interfaces)
def test_get_team_name(db: Dataset) -> None:
    """It should return the name of the team with a given ID."""
    team_name = db.get_team_name(team_id=782)
    assert team_name == "Belgium"
    with pytest.raises(LookupError, match="No team found with ID=0"):
        db.get_team_name(team_id=0)


@pytest.mark.parametrize("db", database_interfaces)
def test_search_team(db: Dataset) -> None:
    """It should return a list with matching teams."""
    teams = db.search_team("Belg").set_index("team_id")
    assert len(teams) == 1
    assert teams.at[782, "team_name"] == "Belgium"


def test_fsspec_adapter() -> None:
    """It should correctly infer protocols and perform I/O operations."""
    adapter = FSSpecAdapter()
    assert adapter._infer_protocol("memory://test") == "memory"
    assert adapter._infer_protocol("s3://bucket/test") == "s3"
    assert adapter._infer_protocol("http://example.com/file.csv") == "http"
    assert adapter._infer_protocol("/local/path/file.csv") == "file"
    assert adapter.supports("memory://test")
    assert adapter.supports("/local/path")

    url = "memory://adapter_test/file.txt"
    with adapter.open(url, "wb") as f:
        f.write(b"socceraction fsspec test")
    assert adapter.exists(url)
    assert adapter.is_file(url)
    assert not adapter.is_directory(url)
    with adapter.open(url, "rb") as f:
        assert f.read() == b"socceraction fsspec test"


def test_parquet_dask_engine(parquetdb: ParquetDataset) -> None:
    """It should support reading tables using dask engine."""
    import dask.dataframe as dd

    ddf_games = parquetdb.read_table("games", engine="dask")
    assert isinstance(ddf_games, dd.DataFrame)
    assert len(ddf_games.compute()) == 1

    ddf_events = parquetdb.read_table("events", engine="dask")
    assert isinstance(ddf_events, dd.DataFrame)
    assert len(ddf_events.compute()) > 0


def test_sql_transaction_rollback_on_insert_partition_failure() -> None:
    """It should roll back partition deletion if subsequent insert fails."""
    engine = create_engine("sqlite://")
    part = PartitionIdentifier(competition_id=1, season_id=1, game_id=10)
    df_orig = pd.DataFrame(
        {"competition_id": [1], "season_id": [1], "game_id": [10], "val": ["orig"]}
    )

    with SQLDataset(engine) as db:
        db.insert_partition_table("test_part", df_orig, part)
        assert len(db.read_table("test_part", partitions=[part])) == 1

        df_new = pd.DataFrame(
            {"competition_id": [1], "season_id": [1], "game_id": [10], "val": ["new"]}
        )
        with patch.object(db, "to_sql", side_effect=RuntimeError("Disk write error")):
            with pytest.raises(RuntimeError, match="Disk write error"):
                db.insert_partition_table("test_part", df_new, part)

        # Previous partition data must still be intact
        remaining = db.read_table("test_part", partitions=[part])
        assert len(remaining) == 1
        assert remaining.iloc[0]["val"] == "orig"


def test_sql_transaction_rollback_on_upsert_failure() -> None:
    """It should roll back inserted data if deduplication fails."""
    engine = create_engine("sqlite://")
    df_orig = pd.DataFrame({"player_id": [1], "player_name": ["Alice"]})

    with SQLDataset(engine) as db:
        db.upsert_table("players", df_orig, ["player_id"])
        assert len(db.read_table("players")) == 1

        df_new = pd.DataFrame({"player_id": [2], "player_name": ["Bob"]})
        with patch.object(db, "_drop_duplicates", side_effect=RuntimeError("Dedup failed")):
            with pytest.raises(RuntimeError, match="Dedup failed"):
                db.upsert_table("players", df_new, ["player_id"])

        # Rollback should ensure Bob was not committed
        players = db.read_table("players")
        assert len(players) == 1
        assert players.iloc[0]["player_name"] == "Alice"


def test_sql_outer_transaction_and_nested_savepoint() -> None:
    """It should support outer transactions and nested savepoints."""
    engine = create_engine("sqlite://")
    with SQLDataset(engine) as db:
        # 1. Full rollback of outer transaction
        try:
            with db.transaction():
                db.to_sql(pd.DataFrame({"a": [1]}), "t_outer", index=False)
                raise RuntimeError("Outer failure")
        except RuntimeError:
            pass
        assert not db.has_table("t_outer") or len(db.read_table("t_outer")) == 0

        # 2. Nested savepoint rollback while outer transaction commits
        with db.transaction():
            db.to_sql(pd.DataFrame({"a": [1]}), "t_nested", index=False)
            try:
                with db.transaction():
                    db.to_sql(
                        pd.DataFrame({"a": [2]}), "t_nested", if_exists="append", index=False
                    )
                    raise ValueError("Inner failure")
            except ValueError:
                pass
            db.to_sql(pd.DataFrame({"a": [3]}), "t_nested", if_exists="append", index=False)

        res = db.read_table("t_nested")
        assert res["a"].tolist() == [1, 3]


def test_sql_context_manager_commit_and_rollback(tmp_path: Path) -> None:
    """It should auto-commit on clean context exit and roll back on error."""
    db_file = tmp_path / "context_test.db"
    uri = f"sqlite:///{db_file}"

    # Auto-commit on normal exit
    with SQLDataset(uri) as db:
        db.to_sql(pd.DataFrame({"a": [10, 20]}), "t_persist", index=False)

    with SQLDataset(uri) as db_check:
        assert len(db_check.read_table("t_persist")) == 2

    # Roll back on exception
    try:
        with SQLDataset(uri) as db:
            db.to_sql(pd.DataFrame({"a": [30]}), "t_persist", if_exists="append", index=False)
            raise RuntimeError("Crash inside context")
    except RuntimeError:
        pass

    with SQLDataset(uri) as db_check2:
        assert len(db_check2.read_table("t_persist")) == 2


@fixture
def loaded_sqlite_db() -> Generator[SQLDataset, None, None]:
    """Provide a SQLDataset preloaded with a local match."""
    data_dir = Path(__file__).parent.parent / "datasets" / "statsbomb" / "raw"
    data_loader = StatsBombLoader(root=str(data_dir), getter="local")
    engine = create_engine("sqlite://")
    with SQLDataset(engine) as db:
        db.import_data(
            data_loader, PartitionIdentifier(competition_id=55, season_id=43, game_id=3795107)
        )
        yield db


def test_transform_default_tables(loaded_sqlite_db: SQLDataset) -> None:
    """It should use default from_table and to_table from the transformer."""
    db = loaded_sqlite_db
    assert not db.has_table("actions")
    res = db.transform(StatsBombEventsToActions())
    assert res is None
    assert db.has_table("actions")
    df_actions = db.read_table("actions")
    assert len(df_actions) > 0


def test_transform_to_table_none(loaded_sqlite_db: SQLDataset) -> None:
    """It should return the DataFrame and not write to dataset when to_table is None."""
    db = loaded_sqlite_db
    assert not db.has_table("actions")
    df_actions = db.transform(StatsBombEventsToActions(), to_table=None)
    assert isinstance(df_actions, pd.DataFrame)
    assert len(df_actions) > 0
    assert not db.has_table("actions")


def test_transform_override_tables(loaded_sqlite_db: SQLDataset) -> None:
    """It should allow overriding default from_table and to_table."""
    db = loaded_sqlite_db
    # Override to_table: events -> custom_actions
    db.transform(
        StatsBombEventsToActions(),
        to_table="custom_actions",
    )
    assert db.has_table("custom_actions")
    assert not db.has_table("actions")
    df_custom_actions = db.read_table("custom_actions")
    assert len(df_custom_actions) > 0

    # Override from_table and to_table: custom_actions -> custom_atomic
    db.transform(
        ActionsToAtomic(),
        from_table="custom_actions",
        to_table="custom_atomic",
    )
    assert db.has_table("custom_atomic")
    assert not db.has_table("atomic_actions")
    df_custom_atomic = db.read_table("custom_atomic")
    assert len(df_custom_atomic) > 0


def test_transform_missing_from_table() -> None:
    """It should raise ValueError when neither argument nor transformer specifies from_table."""

    class NoDefaultTransform(Transform[pd.DataFrame]):
        def __call__(self, game: object, data: pd.DataFrame) -> pd.DataFrame:
            return data

    engine = create_engine("sqlite://")
    with SQLDataset(engine) as db:
        with pytest.raises(ValueError, match="No 'from_table' specified"):
            db.transform(NoDefaultTransform())
