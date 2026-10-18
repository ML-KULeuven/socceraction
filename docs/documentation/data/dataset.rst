.. currentmodule:: socceraction.data.dataset

=================
Managing datasets
=================

The :mod:`socceraction.data.dataset` module provides a unified database
interface for storing, querying, and transforming raw match event streams,
metadata, and converted SPADL actions. It supports multiple storage backends,
including Parquet files (locally or in cloud storage via `fsspec`), SQL
databases (such as SQLite, PostgreSQL, and MySQL via SQLAlchemy), and HDF5
files.

-----
Setup
-----

Depending on the backend you want to use, you can install the corresponding
optional dependencies:

.. code-block:: console

   # For Parquet and cloud storage (fsspec + pyarrow)
   $ pip install "socceraction[parquet]"

   # For SQL databases (SQLAlchemy)
   $ pip install "socceraction[sql]"

   # For HDF5 storage (PyTables)
   $ pip install "socceraction[hdf]"


------------------
Supported backends
------------------

All dataset implementations inherit from the abstract :class:`Dataset` class,
providing a common API for loading, inspecting, querying, and transforming match
data.


Parquet & Cloud Storage (:class:`ParquetDataset`)
=================================================

:class:`ParquetDataset` stores unpartitioned tables (such as ``competitions``,
``games``, ``teams``, and ``players``) as columnar ``{table}.parquet`` files and
partitioned tables (such as ``events`` and ``actions``) under directory
partitions (e.g., ``events/game_{game_id}.parquet``).

It uses :class:`FSSpecAdapter` to interface with `fsspec <https://filesystem-spec.readthedocs.io/>`__,
enabling transparent access to diverse filesystems without changing your code:

- **Local filesystem:** standard directory paths (e.g., ``"data/parquet_db"``).
- **In-memory filesystem:** ``"memory://dataset_name"`` for fast, ephemeral caching and testing.
- **Cloud object storage:** ``"s3://bucket/path"`` (AWS S3), ``"gcs://bucket/path"`` (Google Cloud Storage), or ``"az://container/path"`` (Azure Blob).
- **Caching:** prefix remote URLs with ``simplecache::`` (e.g., ``"simplecache::s3://my-bucket/dataset"``) to cache remote Parquet files locally on disk.

Connecting to a Parquet dataset:

.. code-block:: python

   from socceraction.data import ParquetDataset

   # Local directory
   db_local = ParquetDataset("data/my_dataset")

   # In-memory store
   db_mem = ParquetDataset("memory://euro2020")

   # Remote cloud storage with custom storage options
   db_s3 = ParquetDataset(
       "s3://my-soccer-bucket/datasets/epl",
       storage_options={"key": "AWS_KEY", "secret": "AWS_SECRET"},
   )

   # Or using a context manager
   with ParquetDataset("data/my_dataset") as db:
       df_games = db.games()

:class:`ParquetDataset` supports two execution engines:
- ``engine="pandas"`` (default): loads tables and partitions using PyArrow into standard Pandas DataFrames.
- ``engine="dask"``: lazily reads tables and partitions using `Dask <https://www.dask.org/>`__ DataFrames for large-scale, out-of-core computations.


SQL Databases (:class:`SQLDataset`)
===================================

:class:`SQLDataset` interfaces with relational databases via SQLAlchemy. You can
connect to SQLite, PostgreSQL, MySQL, or any other SQL dialect supported by
SQLAlchemy by providing a connection URL or engine:

.. code-block:: python

   from socceraction.data import SQLDataset

   # SQLite database file
   with SQLDataset("sqlite:///data/soccer.db") as db:
       df_competitions = db.competitions()

   # PostgreSQL database
   db = SQLDataset("postgresql+psycopg2://user:password@localhost:5432/socceraction")

:class:`SQLDataset` provides transactional safety:
- Write operations (such as :meth:`~SQLDataset.upsert_table`, :meth:`~SQLDataset.insert_partition_table`, and :meth:`~Dataset.import_data`) execute inside transactions.
- Replacing partition data is atomic: previous partition records are only deleted if the replacement write succeeds.
- Nested transactions use database savepoints, allowing granular error handling.
- When used as a context manager, uncommitted changes are automatically committed on clean exit and rolled back if an unhandled exception occurs.


HDF5 Store (:class:`HDFDataset`)
================================

:class:`HDFDataset` provides high-performance binary storage on local disk using
PyTables. Partitioned tables are stored hierarchically under keys such as
``/events/game_{game_id}``:

.. code-block:: python

   from socceraction.data import HDFDataset

   with HDFDataset("data/soccer.h5", mode="a") as db:
       df_games = db.games()


--------------
Importing data
--------------

You can import match data directly from any :class:`~socceraction.data.providers.base.EventDataLoader`
(such as :class:`~socceraction.data.providers.statsbomb.StatsBombLoader`,
:class:`~socceraction.data.providers.wyscout.WyscoutLoader`, or
:class:`~socceraction.data.providers.opta.OptaLoader`) using the :meth:`~Dataset.import_data` method.

The :class:`PartitionIdentifier` allows you to select specific competitions,
seasons, or games to import:

.. code-block:: python

   from socceraction.data import ParquetDataset, PartitionIdentifier, StatsBombLoader

   # Initialize data loader and dataset
   loader = StatsBombLoader(getter="remote")
   db = ParquetDataset("data/euro2020_parquet")

   # Import a single game
   db.import_data(
       loader,
       partition=PartitionIdentifier(competition_id=55, season_id=43, game_id=3795107),
   )

   # Import an entire competition season
   db.import_data(
       loader,
       partition=PartitionIdentifier(competition_id=55, season_id=43),
   )

   # Import all available data from the loader
   db.import_data(loader)

Importing data is idempotent: existing records are automatically updated or
overwritten without creating duplicate entries.


-------------------------
Querying & accessing data
-------------------------

Metadata queries
================

The dataset provides convenience methods for querying core entities:

.. code-block:: python

   # Retrieve DataFrames for competitions and games
   df_competitions = db.competitions()
   df_games = db.games()

   # Retrieve teams and players participating in the dataset
   df_teams = db.teams()
   df_players = db.players()

   # Search for a player or team by name (case-insensitive substring match)
   kevin = db.search_player("Kevin")
   manchester = db.search_team("Manchester")

   # Lookup helper methods
   name = db.get_player_name(player_id=3089)
   home_team_id, away_team_id = db.get_home_away_team_id(game_id=3795107)


Event streams & partitions
==========================

To retrieve the raw event stream of a specific game:

.. code-block:: python

   df_events = db.events(game_id=3795107)

You can also read generic tables and filter by partitions:

.. code-block:: python

   # Read the events of multiple games
   partitions = [
       PartitionIdentifier(game_id=3795107),
       PartitionIdentifier(game_id=3795220),
   ]
   df_multi_events = db.read_table("events", partitions=partitions)


Dictionary-style table access
=============================

Datasets implement dictionary-style indexing and containment checks:

.. code-block:: python

   # Check if a table exists
   if "actions" in db:
       df_actions = db["actions"]

   # Read a table by key
   df_games = db["games"]


------------------------
Transforming match data
------------------------

The :meth:`~Dataset.transform` method allows applying a transformation pipeline
across games in the dataset and storing the transformed output in a new table.

Transformers specify default ``from_table`` and ``to_table`` attributes, making
these arguments optional:

.. code-block:: python

   from socceraction.data.transforms import StatsBombEventsToActions

   # Convert raw events to SPADL actions for all games in the dataset
   # (defaults to from_table="events" and to_table="actions")
   db.transform(StatsBombEventsToActions())

   # Retrieve converted SPADL actions
   df_actions = db.read_table("actions")

You can also explicitly override the source and target tables:

.. code-block:: python

   db.transform(
       StatsBombEventsToActions(),
       from_table="events",
       to_table="actions",
   )

If you want the transformed data returned directly as a DataFrame without
saving it to a table, set ``to_table=None``:

.. code-block:: python

   df_actions = db.transform(StatsBombEventsToActions(), to_table=None)

To limit the transformation to specific competitions or games, pass a list of
:class:`PartitionIdentifier` objects:

.. code-block:: python

   selected_partitions = [
       PartitionIdentifier(competition_id=55, season_id=43),
   ]
   db.transform(
       StatsBombEventsToActions(),
       partitions=selected_partitions,
   )

For large datasets, you can use ``engine="dask"`` to execute the transformations
in parallel or out-of-core:

.. code-block:: python

   ddf_actions = db.transform(
       StatsBombEventsToActions(),
       to_table=None,  # returns the transformed Dask DataFrame
       engine="dask",
   )


--------------------
Transactional safety
--------------------

When using :class:`SQLDataset`, you can wrap multiple operations in an atomic
transaction using the :meth:`~Dataset.transaction` context manager:

.. code-block:: python

   with SQLDataset("sqlite:///data/soccer.db") as db:
       with db.transaction():
           db.upsert_table("teams", df_teams, primary_keys=["team_id"])
           db.upsert_table("players", df_players, primary_keys=["player_id"])
           db.import_data(loader, partition=PartitionIdentifier(game_id=3795107))

If any step within the block raises an error, all changes made within that
transaction are automatically rolled back, leaving the database consistent.
