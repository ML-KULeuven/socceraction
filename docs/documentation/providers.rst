**************
Data providers
**************

Socceraction provides :ref:`loaders <api-data>` for various popular data sources that enable
loading events and corresponding metadata as Pandas DataFrames using a unified
data model. Currently, the following data sources are supported:

.. list-table::
   :widths: 25 25 50
   :header-rows: 1

   * - Data source
     - Loader
     - Usage
   * - StatsBomb API
     - :class:`~socceraction.data.statsbomb.StatsBombLoader`
     - ``StatsBombLoader(getter="remote", creds={"user": "", "passwd": ""})``
   * - StatsBomb public data
     - :class:`~socceraction.data.statsbomb.StatsBombLoader`
     - ``StatsBombLoader(getter="remote", creds=None)``
   * - StatsBomb local data
     - :class:`~socceraction.data.statsbomb.StatsBombLoader`
     - ``StatsBombLoader(getter="local", root="data/statsbomb")``
   * - Wyscout API
     - :class:`~socceraction.data.wyscout.WyscoutLoader`
     - ``WyscoutLoader(getter="remote", creds={"user": "", "passwd": ""})``
   * - Wyscout local data
     - :class:`~socceraction.data.wyscout.WyscoutLoader`
     - ``WyscoutLoader(getter="local", root="data/wyscout")``
   * - Wyscout public data
     - :class:`~socceraction.data.wyscout.PublicWyscoutLoader`
     - ``PublicWyscoutLoader(root="data/wyscout-public")``
   * - Opta F7 and F24 XML feeds
     - :class:`~socceraction.data.opta.OptaLoader`
     - ``OptaLoader(root="data/opta", parser="xml")``
   * - Opta F1, F9 and F24 JSON feeds
     - :class:`~socceraction.data.opta.OptaLoader`
     - ``OptaLoader(root="data/opta", parser="json")``
   * - Stats Perform MA1 and MA3 JSON feeds
     - :class:`~socceraction.data.opta.OptaLoader`
     - ``OptaLoader(root="data/statsperform", parser="statsperform")``
   * - WhoScored data
     - :class:`~socceraction.data.opta.OptaLoader`
     - ``OptaLoader(root="data/whoscored", parser="whoscored")``

Each :class:`~socceraction.data.base.Loader` class provides the following
methods:

.. list-table::
   :widths: 25 25 50
   :header-rows: 1

   * - Method
     - Parameters
     - Description
   * - :meth:`~socceraction.data.base.Loader.competitions`
     -
     - All available competitions and seasons
   * - :meth:`~socceraction.data.base.Loader.games`
     - ``competition_id``, ``season_id``
     - All available games in a season
   * - :meth:`~socceraction.data.base.Loader.teams`
     - ``game_id``
     - Both teams that participated in a game
   * - :meth:`~socceraction.data.base.Loader.players`
     - ``game_id``
     - All players that participated in a game
   * - :meth:`~socceraction.data.base.Loader.events`
     - ``game_id``
     - The event stream of a game

Refer to the :ref:`API reference <api-data>` for detailed instructions on
how to use each loader.
