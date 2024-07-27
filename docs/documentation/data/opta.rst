.. currentmodule:: socceraction.data.providers.opta

=========================
Loading Opta data
=========================

`Opta's event stream data`_ comes in many different flavours. The
:class:`OptaLoader` class provides an API client enabling you to fetch
data from the following data feeds as Pandas DataFrames:

- Opta F1, F9 and F24 JSON feeds
- Opta F7 and F24 XML feeds
- StatsPerform MA1 and MA3 JSON feeds
- WhoScored.com JSON data

Currently, only loading data from local files is supported.

--------------------------
Connecting to a data store
--------------------------

First, you have to create a :class:`OptaLoader` object and configure it
for the data feeds you want to use.

Generic setup
=============

To set up a :class:`OptaLoader` you have to specify the root
directory, the filename hierarchy of the feeds and a parser for each feed.
For example::

  from socceraction.data.opta import OptaLoader, parsers

  api = OptaLoader(
    root="data/opta",
    feeds = {
        "f7": "f7-{competition_id}-{season_id}-{game_id}.xml",
        "f24": "f24-{competition_id}-{season_id}-{game_id}.xml",
    }
    parser={
        "f7": parsers.F7XMLParser,
        "f24": parsers.F24XMLParser
    }
  )


Since the loader uses the directory structure and file names to determine
which files should be parsed, the root directory should have a predefined
file hierarchy defined in the ``feeds`` argument. A wide range of file names
and directory structures are supported. However, the competition, season, and
game identifiers must be included in the file names to be able to locate the
corresponding files for each entity. For example, you might have grouped feeds
by competition and season as follows::

  root
  ├── competition_<competition_id>
  │   ├── season_<season_id>
  │   │   ├── f7_<game_id>.xml
  │   │   └── f24_<game_id>.xml
  │   └── ...
  └── ...

In this case, you can use the following feeds configuration::

    feeds = {
        "f7": "competition_{competition_id}/season_{season_id}/f7_{game_id}.xml",
        "f24": "competition_{competition_id}/season_{season_id}/f24_{game_id}.xml",
    }

.. note::

   On Windows, the backslash character should be used as a path separator.

Furthermore, a few standard configurations are provided. These are listed below.


Opta F7 and F24 XML feeds
=========================

.. code-block:: python

  from socceraction.data.opta import OptaLoader

  api = OptaLoader(root="data/opta", parser="xml")

The root directory should have the following structure:

.. code-block::

  root
  ├── f7-{competition_id}-{season_id}.xml
  ├── f24-{competition_id}-{season_id}-{game_id}.xml
  └── ...


Opta F1, F9 and F24 JSON feeds
==============================

.. code-block:: python

  from socceraction.data.opta import OptaLoader

  api = OptaLoader(root="data/opta", parser="json")

The root directory should have the following structure:

.. code-block::

  root
  ├── f1-{competition_id}-{season_id}.json
  ├── f9-{competition_id}-{season_id}.json
  ├── f24-{competition_id}-{season_id}-{game_id}.json
  └── ...

StatsPerform MA1 and MA3 JSON feeds
===================================

.. code-block:: python

  from socceraction.data.opta import OptaLoader

  api = OptaLoader(root="data/statsperform", parser="statsperform")

The root directory should have the following structure:

.. code-block::

  root
  ├── ma1-{competition_id}-{season_id}.json
  ├── ma3-{competition_id}-{season_id}-{game_id}.json
  └── ...


WhoScored
=========

`WhoScored.com`_ is a popular website that provides detailed live match statistics.
These statistics are compiled from Opta's event feed, which can be scraped
from the website's source code using a library such as `soccerdata`_. Once you
have downloaded the raw JSON data, you can parse it using the :class:`OptaLoader`
with:

.. code-block:: python

  from socceraction.data.opta import OptaLoader

  api = OptaLoader(root="data/whoscored", parser="whoscored")

The root directory should have the following structure:

.. code-block::

  root
  ├── {competition_id}-{season_id}-{game_id}.json
  └── ...


Alternatively, the soccerdata library provides a wrapper that immediately
returns a :class:`OptaLoader` object for a scraped dataset.

.. code-block:: python

  import soccerdata as sd

  # Setup a scraper for the 2021/2022 Premier League season
  ws = sd.WhoScored(leagues="ENG-Premier League", seasons=2021)
  # Scrape all games and return a OptaLoader object
  api = ws.read_events(output_fmt='loader')


.. warning::

   Scraping data from WhoScored.com violates their terms of service. Legally,
   scraping this data is therefore a grey area. If you decide to use this
   data anyway, this is your own responsibility.


------------
Loading data
------------

Next, you can load the match event stream data and metadata by calling the
corresponding methods on the :class:`OptaLoader` object.

- :func:`OptaLoader.competitions()`
- :func:`OptaLoader.games()`
- :func:`OptaLoader.teams()`
- :func:`OptaLoader.players()`
- :func:`OptaLoader.events()`

.. _Opta's event stream data: https://www.statsperform.com/opta-event-definitions/
.. _soccerdata: https://soccerdata.readthedocs.io/en/latest/datasources/WhoScored.html
.. _WhoScored.com: https://www.whoscored.com/
