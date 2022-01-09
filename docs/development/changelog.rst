=========
Changelog
=========

All notable changes to this project will be documented in this file.

Unreleased_
============

1.2.1_ - 2022-01-09
===================

Fixed
-----
- SchemaErrors on Windows (`GH157 <https://github.com/ML-KULeuven/socceraction/issues/157>`_)


1.2.0_ - 2022-01-07
===================

Added
-----
- Add support for loading StatsBomb 360 data
- Add support for loading StatsBomb data directly from the paid API
- Add documentation for the data module
- Add documentation for the StatsBomb data schemas
- Add documentation for the Opta data schemas

Changed
-------
- Remove the requests dependency
- Remove the Unicode dependency
- Some fields in the Opta data schema were removed or renamed to make them
  more uniform with the other data loaders
- The ``referee_id`` and ``venue_id`` fields were replaced by a ``referee`` and
  ``venue`` field, respectively

Fixed
-----
- Fix a bug in the "minutes_played" field of the dataframe returned by the socceraction.data.wyscout.PublicWyscoutLoader.players method. (`GH153 <https://github.com/ML-KULeuven/socceraction/issues/153>`_)
- Add missing WhoScored type ID (`GH143 <https://github.com/ML-KULeuven/socceraction/issues/143>`_)
- Update the MA1 and MA3 Stats Perform parsers to make them compatible with the latest API version
- Several small fixes in the Opta parsers

1.1.3_ - 2021-12-20
===================

Fixed
-----
-  Support loading new StatsBomb event data containing 360 snaphots.

1.1.2_ - 2021-12-08
===================

Added
-----
- Use Poetry for depency management.
- Use Nox for testing multiple Python versions.
- Automatic depandency updates with Depandabot.

Changed
-------
- Drop support for Python 3.6
- Updated README.md
- Updated CONTRIBUTING.md
- Improved CI workflow with deployment to test PyPi and test coverage.

Fixed
-----
- Bugs in the StatsPerform MA3 parser affecting `extract_players()`.
- Conversion to Atomic-SPADL changed the data type of the `player_id` column to `float`.
- Fix incorrect type annnotations.
- Wyscout action coordinates could be outside the [0, 68] or [0, 105] range.
- Moved broken CI workflow from Travis to Github Actions.


1.1.1_ - 2021-09-22
====================

Fixed
-----
- Add missing `data` module to pip release.


1.1.0_ - 2021-09-17
====================

Added
-----
- Support for Stats Perform's MA1 and MA3 JSON feeds by `@JanVanHaaren <https://github.com/JanVanHaaren>`__ and `@denisov-vlad <https://github.com/denisov-vlad>`__
- Enhanced tests suite by use of ``@slow`` decorator, which is controlled via a ``--skip-slow`` command line argument.
- A `play_left_to_right()` function to `socceraction.spadl` and `socceraction.atomic.spadl` which changes the start and end location of each action such that all actions are performed as if the team plays from left to right.
- A `load_model()` function to `socceraction.xthreat` to load a precomputed xT grid

Changed
-------
- Own goals are converted to a "bad_touch" SPADL event with outcome "owngoal". Previously, the action type differed between providers. (`GH26 <https://github.com/ML-KULeuven/socceraction/issues/26>`_)
- All event data loaders are moved from `socceraction.spadl` to `socceraction.data`
- `socceraction.xthreat.ExpectedThreat.predict` is depracated and replaced by `socceraction.xthreat.ExpectedThreat.rate` to be compatible with the VAEP api

Fixed
-----
- Bugs in OptaLoader's `extract_lineups()` function affecting "is_starter" & "minutes_played" columns (`GH48 <https://github.com/ML-KULeuven/socceraction/issues/48>`_)

1.0.2_ - 2021-04-03
====================

Fixed
-----
- Opta converter converted goalkicks to regular passes (`GH45 <https://github.com/ML-KULeuven/socceraction/issues/45>`_)
- Fix StatsBomb converter for the public CL dataset (`GH46 <https://github.com/ML-KULeuven/socceraction/issues/46>`_)
- The `goalscore` feature also counted goalkicks as goals

1.0.1_ - 2021-01-16
====================

Changed
-------
- The WhoScored parser extracts the 'competition_id', 'season_id' and
  'game_id' fields from the filename. It is no longer required to append these
  fields to the JSON.

1.0.0_ - 2021-01-11
====================

Added
-----
- Sphinx documentation
- A `PublicWyscoutLoader` class which enables easy access to the open source Wyscout soccer-logs dataset (`GH14 <https://github.com/ML-KULeuven/socceraction/issues/14>`_)
- A new bodypart type "head/other", since Wyscout does not distinguish beteen
  headers and other body parts (`GH27 <https://github.com/ML-KULeuven/socceraction/issues/27>`_)
- Unit tests for the StatsBomb, Opta and Wyscout data convertors.
- Add an `original_event_id` column to the SPADL format (`GH7 <https://github.com/ML-KULeuven/socceraction/issues/7>`_)
- Add an `action_id` column to Opta and Wyscout SPADL to be consistent with the StatsBomb converter
- A high-level API for training VAEP and Atomic-VAEP models
- A parser for WhoScored JSON
- CI with Travis
- A logo
- Minimal version requirements for dependencies

Changed
-------
- Opta and Wyscout convertors are refactored as a class based API to be
  consistent with the StatsBomb converter (`GH23 <https://github.com/ML-KULeuven/socceraction/issues/23>`_)
- Details in the README are moved to the docs

Fixed
-----
- Check for same period when adding dribbles.
- Fix typo in StatsBomb converter
- Fix type of return value in xthreat.predict
- Fix 'time_seconds' field in the StatsBomb converter for overtime periods and
  shoutouts
- Fix result of Wyscout interception passes (`GH28 <https://github.com/ML-KULeuven/socceraction/issues/28>`_)
- Fix own goals from bad touch events (`GH25 <https://github.com/ML-KULeuven/socceraction/issues/25>`_)

0.2.1_ - 2020-06-16
====================

Fixed
-----
- Use the atomic version of actiontypes in Atomic-SPADL

0.2.0_ - 2020-06-15
====================

Added
-----
- Atomic-SPADL and Atomic-VAEP

Changed
-------
- Rename `socceraction.classification` to `socceraction.vaep`

0.1.1_ - 2020-01-30
====================

Added
-----
- mypy typhinting

Fixed
-----
- Add missing requests dependency


0.1.0_ - 2020-01-22
====================

Changed
-------
- Simpler and more transparant API for the StatsBomb converter

0.0.9_ - 2020-01-14
====================

Added
-----
- Expected threat (xT) implementation by `@MaaikeVR <https://github.com/MaaikeVR>`__ and `@karunsingh <https://github.com/karunsingh>`__

Fixed
-----
- Information leakage in xG model
- Fix end coordinates of clearances


0.0.8_ - 2019-11-29
====================

Fixed
-----
- Remove ujson from setup.py

0.0.7_ - 2019-11-28
====================

Added
-----
- Expected goals demo

Fixed
-----
- Possibility of extra time periods in the Wyscout converter by `@dbelcham <https://github.com/dbelcham>`__
- Fix utf-8 endcoding errors in convertors by `@dbelcham <https://github.com/dbelcham>`__
- Retrieval of Wyscout substitutions by `@dbelcham <https://github.com/dbelcham>`__
- Incorrect "bad touch" event type name in Opta parser
- Fix SIGKDD citation in the readme
- Fix storage of events in the optastore

Removed
-------
- ujson dependency

0.0.6_ - 2019-10-15
====================

Fixed
-----
- Typo in statsbomb.py
- Fixed "scores" and "concedes" label of the last action in a dataframe

0.0.5_ - 2019-10-15
====================

Changed
-------
- Improve speed and memory usage of the StatsBomb converter
- Improve README
- Add `action_id` column to spadl action table

Fixed
-----
- Fixed Opta bugs related to fouls and playergamestats

0.0.4_ - 2019-10-01
====================

Fixed
-----
- Fixed encoding error in StatsBomb parser by `@kim-younghan <https://github.com/kim-younghan>`__
- Fixed `start_angle_to_goal` and `end_angle_to_goal` features

Removed
-------
- LICENCE.txt

0.0.3_ - 2019-08-26
====================

Added
-----
- SPADL convertor for Wyscout event data
- MIT License file
- setup.py file

0.0.2_ - 2019-07-31
====================

0.0.1_ - 2019-07-31
====================

Initial release.

.. _Unreleased: https://github.com/ML-KULeuven/socceraction/compare/v1.2.1...HEAD
.. _1.2.1: https://github.com/ML-KULeuven/socceraction/compare/v1.2.0...v1.2.1
.. _1.2.0: https://github.com/ML-KULeuven/socceraction/compare/v1.1.3...v1.2.0
.. _1.1.3: https://github.com/ML-KULeuven/socceraction/compare/v1.1.2...v1.1.3
.. _1.1.2: https://github.com/ML-KULeuven/socceraction/compare/v1.1.1...v1.1.2
.. _1.1.1: https://github.com/ML-KULeuven/socceraction/compare/v1.1.0...v1.1.1
.. _1.1.0: https://github.com/ML-KULeuven/socceraction/compare/v1.0.2...v1.1.0
.. _1.0.2: https://github.com/ML-KULeuven/socceraction/compare/v1.0.1...v1.0.2
.. _1.0.1: https://github.com/ML-KULeuven/socceraction/compare/v1.0.0...v1.0.1
.. _1.0.0: https://github.com/ML-KULeuven/socceraction/compare/v0.2.1...v1.0.0
.. _0.2.1: https://github.com/ML-KULeuven/socceraction/compare/v0.2.0...v0.2.1
.. _0.2.0: https://github.com/ML-KULeuven/socceraction/compare/v0.1.1...v0.2.0
.. _0.1.1: https://github.com/ML-KULeuven/socceraction/compare/v0.1.0...v0.1.1
.. _0.1.0: https://github.com/ML-KULeuven/socceraction/compare/v0.0.9...v0.1.0
.. _0.0.9: https://github.com/ML-KULeuven/socceraction/compare/v0.0.8...v0.0.9
.. _0.0.8: https://github.com/ML-KULeuven/socceraction/compare/v0.0.7...v0.0.8
.. _0.0.7: https://github.com/ML-KULeuven/socceraction/compare/v0.0.6...v0.0.7
.. _0.0.6: https://github.com/ML-KULeuven/socceraction/compare/v0.0.5...v0.0.6
.. _0.0.5: https://github.com/ML-KULeuven/socceraction/compare/v0.0.4...v0.0.5
.. _0.0.4: https://github.com/ML-KULeuven/socceraction/compare/v0.0.3...v0.0.4
.. _0.0.3: https://github.com/ML-KULeuven/socceraction/compare/v0.0.2...v0.0.3
.. _0.0.2: https://github.com/ML-KULeuven/socceraction/compare/v0.0.1...v0.0.2
.. _0.0.1: https://github.com/ML-KULeuven/socceraction/releases/tag/v0.0.1
