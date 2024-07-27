.. _api-data:

socceraction.data
=================

socceraction.data.dataset
-------------------------

.. autosummary::
  :toctree: generated
  :nosignatures:
  :template: class.rst

  ~socceraction.data.dataset.HDFDataset
  ~socceraction.data.dataset.SQLDataset


socceraction.data.transforms
----------------------------


.. autosummary::
  :toctree: generated
  :nosignatures:

  ~socceraction.data.transforms.PlayActionsLeftToRight
  ~socceraction.data.transforms.PlayAtomicActionsLeftToRight
  ~socceraction.data.transforms.StatsBombEventsToActions
  ~socceraction.data.transforms.OptaEventsToActions
  ~socceraction.data.transforms.WyscoutEventsToActions
  ~socceraction.data.transforms.ActionsToFeatures
  ~socceraction.data.transforms.StatsBombEventsToFeatures



socceraction.data.providers
---------------------------

.. list-table::
   :widths: 30 70

   * - :ref:`StatsBomb <api-data-statsbomb>`
     - Module for loading StatsBomb event data
   * - :ref:`Opta <api-data-opta>`
     - Module for loading Opta event data and the derived formats used by Stats
       Perform and WhoScored
   * - :ref:`Wyscout <api-data-wyscout>`
     - Module for loading Wyscout event data



.. toctree::
  :hidden:
  :maxdepth: 1

  data_base
  data_statsbomb
  data_opta
  data_wyscout
