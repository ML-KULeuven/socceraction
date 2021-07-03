=========
Changelog
=========

All notable changes to this project will be documented in this file.

Unreleased_
============

Changed
-------
- Own goals are converted to a "bad_touch" SPADL event with outcome "owngoal". Previously, the action type differed between providers. (`GH26 <https://github.com/ML-KULeuven/socceraction/issues/26>`)

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

.. _Unreleased: https://github.com/ML-KULeuven/socceraction/compare/v1.0.2...HEAD
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

