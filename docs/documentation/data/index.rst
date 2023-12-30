.. currentmodule:: socceraction.data

*************
Loading data
*************

Socceraction provides API clients for various popular event stream data
sources. These clients enable fetching event streams and their corresponding
metadata as Pandas DataFrames using a unified data model.
Alternatively, you can also use `kloppy <https://kloppy.pysport.org/>`__ to
load data.

Loading data with socceraction
==============================

All API clients implemented in socceraction inherit from the
:class:`~base.EventDataLoader` interface. This interface provides the
following methods to retrieve data as a Pandas DataFrames with a unified data
model (i.e., :class:`~pandera.Schema`). The schema defines the minimal set of
columns and their types that are returned by each method. Implementations of
the :class:`~base.EventDataLoader` interface may add additional columns.

.. list-table::
   :widths: 40 20 40
   :header-rows: 1

   * - Method
     - Output schema
     - Description
   * - :meth:`competitions() <base.EventDataLoader.competitions>`
     - :class:`~schema.CompetitionSchema`
     - All available competitions and seasons
   * - :meth:`games(competition_id, season_id) <base.EventDataLoader.games>`
     - :class:`~schema.GameSchema`
     - All available games in a season
   * - :meth:`teams(game_id) <base.EventDataLoader.teams>`
     - :class:`~schema.TeamSchema`
     - Both teams that participated in a game
   * - :meth:`players(game_id) <base.EventDataLoader.players>`
     - :class:`~schema.PlayerSchema`
     - All players that participated in a game
   * - :meth:`events(game_id) <base.EventDataLoader.events>`
     - :class:`~schema.EventSchema`
     - The event stream of a game

Currently, the following data providers are supported:

.. toctree::
  :maxdepth: 1

  statsbomb
  wyscout
  opta


Loading data with kloppy
=========================

Similarly to socceraction, `kloppy <https://kloppy.pysport.org/>`__ implements
a unified data model for soccer data. The main differences between kloppy and
socceraction are: (1) kloppy supports more data sources (including tracking
data), (2) kloppy uses a more flexible object-based data model in contrast to
socceraction's dataframe-based model, and (3) kloppy covers a more complete
set of events while socceraction focuses on-the-ball events. Thus, we recommend
using kloppy if you want to load data from a source that is not supported by
socceraction or when your analysis is not limited to on-the-ball events.

The following code snippet shows how to load data from StatsBomb using
kloppy::

  from kloppy import statsbomb

  dataset = statsbomb.load_open_data(match_id=8657)

Instructions for loading data from other sources can be found in the
`kloppy documentation <https://kloppy.pysport.org/>`__.

You can then convert the data to the SPADL format using the
:func:`~socceraction.spadl.kloppy.convert_to_actions` function::

  from socceraction.spadl.kloppy import convert_to_actions

  spadl_actions = convert_to_actions(dataset, game_id=8657)


.. note::

   Currently, the data model of kloppy is only complete for StatsBomb data.
   If you use kloppy to load data from other sources and convert it to the
   SPADL format, you may lose some information.
