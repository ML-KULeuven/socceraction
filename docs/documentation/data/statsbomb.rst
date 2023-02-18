.. currentmodule:: socceraction.data.statsbomb

=========================
Loading StatsBomb data
=========================

The :class:`StatsBombLoader` class provides an API client enabling you to
fetch `StatsBomb event stream data`_ as Pandas DataFrames. This document provides
an overview of the available data sources and how to access them.

------
Setup
------

To be able to load StatsBomb data, you'll first need to install a few
additional dependencies which are not included in the default installation of
socceraction. You can install these additional dependencies by running:

.. code-block:: console

  $ pip install "socceraction[statsbomb]"


--------------------------
Connecting to a data store
--------------------------

First, you have to create a :class:`StatsBombLoader` object and configure it
for the data store you want to use. The :class:`StatsBombLoader` supports
loading data from the StatsBomb Open Data repository, from the official
StatsBomb API, and from local files.


Open Data repository
====================

StatsBomb has made event stream data of certain leagues freely available for
public non-commercial use at https://github.com/statsbomb/open-data. This open
data can be accessed without the need of authentication, but its use is
subject to a `user agreement`_. The code below shows how to setup an API client
that can fetch data from the repository.

.. code-block:: python

  # optional: suppress warning about missing authentication
  import warnings
  from statsbombpy.api_client import NoAuthWarning
  warnings.simplefilter('ignore', NoAuthWarning)

  from socceraction.data.statsbomb import StatsBombLoader

  api = StatsBombLoader(getter="remote", creds=None)


.. note::
   If you publish, share or distribute any research, analysis or insights based
   on this data, StatsBomb requires you to state the data source as StatsBomb
   and use their logo.


StatsBomb API
=============

API access is for paying customers only. Authentication can be done by setting
environment variables named ``SB_USERNAME`` and ``SB_PASSWORD`` to your login
credentials. Alternatively, the constructor accepts an argument ``creds`` to
pass your login credentials in the format ``{"user": "", "passwd": ""}``.

.. code-block:: python

  from socceraction.data.statsbomb import StatsBombLoader

  # set authentication credentials as environment variables
  import os
  os.environ["SB_USERNAME"] = "your_username"
  os.environ["SB_PASSWORD"] = "your_password"
  api = StatsBombLoader(getter="remote")

  # or provide authentication credentials as a dictionary
  api = StatsBombLoader(getter="remote", creds={"user": "", "passwd": ""})


Local directory
===============

A final option is to load data from a local directory. This local directory
can be specified by passing the ``root`` argument to the constructor,
specifying the path to the local data directory.

.. code-block:: python

  from socceraction.data.statsbomb import StatsBombLoader

  api = StatsBombLoader(getter="local", root="data/statsbomb")

Note that the data should be organized in the same way as the StatsBomb Open
Data repository, which corresponds to the following file hierarchy:

.. code-block::

  root
  ├── competitions.json
  ├── events
  │   ├── <match_id>.json
  │   ├── ...
  │   └── ...
  ├── lineups
  │   ├── <match_id>.json
  │   └── ...
  ├── matches
  │   ├── <competition_id>
  │   │   └── <season_id>.json
  │   │   └── ...
  │   └── ...
  └── three-sixty
      ├── <match_id>.json
      └── ...



------------
Loading data
------------

Next, you can load the match event stream data and metadata by calling the
corresponding methods on the :class:`StatsBombLoader` object.


:func:`StatsBombLoader.competitions()`
======================================

.. code-block:: python

   df_competitions = api.competitions()

.. csv-table::
   :class: dataframe
   :header: season_id,competition_id,competition_name,country_name,competition_gender,season_name

    106,43,FIFA World Cup,International,male,2022
    30,72,Women's World Cup,International,female,2019
    3,43,FIFA World Cup,International,male,2018


:func:`StatsBombLoader.games()`
===============================

.. code-block:: python

   df_games = api.games(competition_id=43, season_id=3)


.. csv-table::
   :class: dataframe
   :header: game_id,season_id,competition_id,competition_stage,game_day,game_date,home_team_id,away_team_id,home_score,away_score,venue,referee_id

    8658,3,43,Final,7,2018-07-15 17:00:00,771,785,4,2,Stadion Luzhniki,730
    8657,3,43,3rd Place Final,7,2018-07-14 16:00:00,782,768,2,0,Saint-Petersburg Stadium,741

:func:`StatsBombLoader.teams()`
===============================

.. code-block:: python

   df_teams = api.teams(game_id=8658)

.. csv-table::
   :class: dataframe
   :header: team_id,team_name
   :align: left

    771,France
    785,Croatia



:func:`StatsBombLoader.players()`
=================================

.. code-block:: python

   df_players = api.players(game_id=8658)


.. csv-table::
   :class: dataframe
   :header: game_id,team_id,player_id,player_name,nickname,jersey_number,is_starter,starting_position_id,starting_position_name,minutes_played

    8658,771,3009,Kylian Mbappé Lottin,Kylian Mbappé,10,True,12,Right Midfield,95
    8658,785,5463,Luka Modrić,,10,True,13,Right Center Midfield,95


:func:`StatsBombLoader.events()`
================================

.. code-block:: python

   df_events = api.events(game_id=8658)

.. csv-table::
   :class: dataframe
   :header: event_id,index,period_id,timestamp,minute,second,type_id,type_name,possession,possession_team_id,possession_team_name,play_pattern_id,play_pattern_name,team_id,team_name,duration,extra,related_events,player_id,player_name,position_id,position_name,location,under_pressure,counterpress,game_id

    47638847-fd43-4656-b49c-cff64e5cfc0a,1,1,1900-01-01,0,0,35,Starting XI,1,771,France,1,Regular Play,771,France,0.0,"{...}",[],,,,,,False,False,8658
    0c04305d-5615-4520-9be5-7c232829954b,2,1,1900-01-01,0,0,35,Starting XI,1,771,France,1,Regular Play,785,Croatia,1.412,"{...}",[],,,,,,False,False,8658
    c5e17439-efe2-480b-9cff-1600998674d7,3,1,1900-01-01,0,0,18,Half Start,1,771,France,1,Regular Play,771,France,0.0,{},['7e1460eb-c572-4059-8cd4-cec4857f818d'],,,,,,False,False,8658


If `360 data snapshots`_ are available for the game, they can be loaded by
passing ``load_360=True`` to the ``events()`` method. This will add two columns
to the events dataframe: ``visible_area_360`` and ``freeze_frame_360``. The
former contains the visible area of the pitch in the 360 snapshot, while the
latter contains the player locations in the 360 snapshot.

.. code-block:: python

   df_events = api.events(game_id=3788741, load_360=True)


.. _StatsBomb event stream data: https://statsbomb.com/what-we-do/soccer-data/
.. _statsbombpy: https://pypi.org/project/statsbombpy/
.. _user agreement: https://github.com/statsbomb/open-data/blob/master/LICENSE.pdf
.. _360 data snapshots: https://statsbomb.com/what-we-do/soccer-data/360-2/
