
.. currentmodule:: socceraction.data.wyscout

=========================
Loading Wyscout data
=========================

The :class:`WyscoutLoader` class provides an API client enabling you to fetch
`Wyscout event stream data`_ as Pandas DataFrames. This document provides an
overview of the available data sources and how to access them.

.. note::

  Currently, only version 2 of the Wyscout API is supported.
  See https://github.com/ML-KULeuven/socceraction/issues/156
  for progress on version 3 support.


--------------------------
Connecting to a data store
--------------------------

First, you have to create a :class:`WyscoutLoader` object and configure it
for the data store you want to use. The :class:`WyscoutLoader` supports
loading data from the official Wyscout API and from local files. Additionally,
the :class:`PublicWyscoutLoader` class can be used to load a publicly
available dataset.


Wyscout API
=============

`Wyscout API <https://apidocs.wyscout.com/>`_ access requires a separate
subscription. Wyscout currently offers `three different packs
<https://footballdata.wyscout.com/packages/>`_: a Database Pack (match sheet
data), a Stats Pack (statistics derived from match event data), and an Events
Pack (raw match event data). A subscription to the  Events Pack is required to
access the event stream data.

Authentication can be done by setting environment variables named
``WY_USERNAME`` and ``WY_PASSWORD`` to your login credentials (i.e., client id
and secret). Alternatively, the constructor accepts an argument ``creds`` to
pass your login credentials in the format ``{"user": "", "passwd": ""}``.


.. code-block:: python

  from socceraction.data.wyscout import WyscoutLoader

  # set authentication credentials as environment variables
  import os
  os.environ["WY_USERNAME"] = "your_client_id"
  os.environ["WY_PASSWORD"] = "your_secret"
  api = WyscoutLoader(getter="remote")

  # or provide authentication credentials as a dictionary
  api = WyscoutLoader(getter="remote", creds={"user": "", "passwd": ""})


Local directory
===============

Data can also be loaded from a local directory. This local directory
can be specified by passing the ``root`` argument to the constructor,
specifying the path to the local data directory.

.. code-block:: python

  from socceraction.data.wyscout import WyscoutLoader

  ap = WyscoutLoader(getter="local", root="data/wyscout")


The loader uses the directory structure and file names to determine which files
should be parsed to retrieve the requested data. Therefore, the local directory
should have a predefined file hierarchy. By default, it expects following file
hierarchy:

.. code-block::

  root
  ├── competitions.json
  ├── seasons_<competition_id>.json
  ├── matches_<season_id>.json
  └── matches
      ├── events_<game_id>.json
      └── ...

If your local directory has a different file hierarchy, you can specify
this custom hierarchy by passing the ``feeds`` argument to the constructor.
A wide range of file names and directory structures are supported. However,
the competition, season, and game identifiers must be included in the file
names to be able to locate the corresponding files for each entity.

.. code-block:: python

  from socceraction.data.wyscout import WyscoutLoader

  ap = WyscoutLoader(getter="local", root="data/wyscout", feeds={
    "competitions": "competitions.json",
    "seasons": "seasons_{competition_id}.json",
    "games": "matches_{season_id}.json",
    "events": "matches/events_{game_id}.json",
  }))

The ``{competition_id}``, ``{season_id}``, and ``{game_id}`` placeholders
will be replaced by the corresponding id values when data is retrieved.


Soccer logs dataset
===================

As part of the "A public data set of spatio-temporal match events in soccer
competitions" paper, Wyscout made an event stream dataset available for
research purposes. The dataset covers the 2017/18 season of the Spanish,
Italian, English, German, and French first division. In addition, it includes
the data of the 2018 World Cup and the 2016 European championship. The dataset
is available at https://figshare.com/collections/Soccer_match_event_dataset/4415000/2.

As the format of this dataset is slightly different from the format of the
official Wyscout API, a separate :class:`PublicWyscoutLoader` class is
provided to load this dataset. This loader will download the dataset once and
extract it to the specified ``root`` directory.


.. code-block:: python

  from socceraction.data.wyscout import PublicWyscoutLoader

  api = PublicWyscoutLoader(root="data/wyscout")


------------
Loading data
------------

Next, you can load the match event stream data and metadata by calling the
corresponding methods on the :class:`WyscoutLoader` object.

- :func:`WyscoutLoader.competitions()`
- :func:`WyscoutLoader.games()`
- :func:`WyscoutLoader.teams()`
- :func:`WyscoutLoader.players()`
- :func:`WyscoutLoader.events()`


.. _Wyscout event stream data: https://footballdata.wyscout.com/
