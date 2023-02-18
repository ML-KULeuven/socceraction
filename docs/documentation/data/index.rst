.. currentmodule:: socceraction.data

*************
Loading data
*************

Socceraction provides API clients for various popular event stream data
sources. These clients enable fetching event streams and their corresponding
metadata as Pandas DataFrames using a unified data model.

The ``EventDataLoader`` interface
=================================

All API  clients inherit from the :class:`~base.EventDataLoader`
interface. This interface provides the following methods to retrieve data
as a Pandas DataFrames with a unified data model (i.e., :class:`~pandera.Schema`).
The schema defines the minimal set of columns and their types that are
returned by each method. Implementations of the :class:`~base.EventDataLoader`
interface may add additional columns.

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


Supported data providers
========================

Currently, the following data providers are supported:

.. toctree::
  :maxdepth: 1

  statsbomb
  wyscout
  opta
