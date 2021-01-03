*********************
Data representation
*********************

Socceraction uses **a tabular action-oriented data format**, as opposed to the
formats by commercial vendors that describe events. The distinction is that
actions are a subset of events that require a player to perform the action.
For example, a passing event is an action, whereas an event signifying the end
of the game is not an action. Unlike all other event stream formats, we always
store the same attributes for each action. Excluding optional information
snippets enables us to store the data in a table and more easily apply
automatic analysis tools.

Socceraction implments two versions of this action-oriented data format: SPADL
and Atomic-SPADL.

SPADL
=====

SPADL (*Soccer Player Action Description Language*) represents a game as
a sequence of on-the-ball actions :math:`[a_1, a_2, . . . , a_m]`, where m is
the total number of actions that happened in the game. Each action is a tuple
of the same twelve attributes:

:game_id:
  the ID of the game in which the action was performed
:period_id:
  the ID of the game period in which the action was performed
:seconds: 
  the action's start time
:player: 
  the player who performed the action
:team: 
  the player's team
:start_x: 
  the x location where the action started
:start_y: 
  the y location where the action started
:end_x: 
  the x location where the action ended
:end_y: 
  the y location where the action ended
:action_type: 
  the type of the action (e.g., pass, shot, dribble)
:result: 
  the result of the action (e.g., success or fail)
:bodypart: 
  the player's body part used for the action


Example
-------

Here is an example of five actions in the SPADL format leading up to Belgium's
second goal against England in the third place play-off in the 2018 FIFA world
cup.

+---------+-----------+---------+---------+----------+----------+----------+--------+--------+------------+---------+----------+
| game_id | period_id | seconds | team    | player   | start\_x | start\_y | end\_x | end\_y | actiontype | result  | bodypart |
+=========+===========+=========+=========+==========+==========+==========+========+========+============+=========+==========+
| 8657    | 2         | 2179    | Belgium | Axel     | 37.1     | 44.8     | 53.8   | 48.2   | pass       | success | foot     |
|         |           |         |         | Witsel   |          |          |        |        |            |         |          |
+---------+-----------+---------+---------+----------+----------+----------+--------+--------+------------+---------+----------+
| 8657    | 2         | 2181    | Belgium | Kevin De | 53.8     | 48.2     | 70.6   | 42.2   | dribble    | success | foot     |
|         |           |         |         | Bruyne   |          |          |        |        |            |         |          |
+---------+-----------+---------+---------+----------+----------+----------+--------+--------+------------+---------+----------+
| 8657    | 2         | 2184    | Belgium | Kevin De | 70.6     | 42.2     | 87.4   | 49.1   | pass       | success | foot     |
|         |           |         |         | Bruyne   |          |          |        |        |            |         |          |
+---------+-----------+---------+---------+----------+----------+----------+--------+--------+------------+---------+----------+
| 8657    | 2         | 2185    | Belgium | Eden     | 87.4     | 49.1     | 97.9   | 38.7   | dribble    | success | foot     |
|         |           |         |         | Hazard   |          |          |        |        |            |         |          |
+---------+-----------+---------+---------+----------+----------+----------+--------+--------+------------+---------+----------+
| 8657    | 2         | 2187    | Belgium | Eden     | 97.9     | 38.7     | 105    | 37.4   | shot       | success | foot     |
|         |           |         |         | Hazard   |          |          |        |        |            |         |          |
+---------+-----------+---------+---------+----------+----------+----------+--------+--------+------------+---------+----------+

Here is the same phase visualized using the ``matplotsoccer`` package

.. figure:: ../eden_hazard_goal.png
   :alt: 


Definitions
-----------

SPADL distinguishes between 22 possible types of actions, up to three different
body parts and up to six possible results. The table below gives the
definition of each action type. The possible body parts are foot,
head, other, and none. For Wyscout, which does not distinguish between the
head and other body parts a special baody part 'head/other' is used. The two
most common results are success or fail, which
indicates whether the action had its intended result or not. For example, a
pass reaching a teammate or a tackle recovering the ball. The four
other possible results are offside for passes resulting in an off-side
call, own goal, yellow card, and red card.


+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Action type        | Description                                      | Success?                | Special result      |
+====================+==================================================+=========================+=====================+
| Pass               | Normal pass in open play                         | Reaches teammate        | Offside             |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Cross              | Cross into the box                               | Reaches teammate        | Offside             |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Throw-in           | Throw-in                                         | Reaches teammate        | /                   |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Crossed free-kick  | Free kick crossed into the box                   | Reaches teammate        | Offside             |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Short free-kick    | Short free-kick                                  | Reaches team mate       | Offside             |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Crossed corner     | Corner crossed into the box                      | Reaches teammate        | /                   |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Short corner       | Short corner                                     | Reaches teammate        | /                   |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Take on            | Attempt to dribble past opponent                 | Keeps possession        | /                   |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Foul               | Foul                                             | Always fail             | Red or yellow card  |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Tackle             | Tackle on the ball                               | Regains possession      | /                   |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Interception       | Interception of the ball                         | Regains possession      | /                   |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Shot               | Shot attempt not from penalty or free-kick       | Goal                    | Own goal            |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Penalty shot       | Penalty shot                                     | Goal                    | /                   |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Free-kick shot     | Direct free-kick on goal                         | Goal                    | /                   |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Keeper save        | Keeper saves a shot on goal                      | Always success          | /                   |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Keeper claim       | Keeper catches a cross                           | Does not drop the ball  | /                   |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Keeper punch       | Keeper punches the ball clear                    | Always success          | /                   |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Keeper pick-up     | Keeper picks up the ball                         | Always success          | /                   |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Clearance          | Player clearance                                 | Always success          | /                   |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Bad touch          | Player makes a bad touch and loses the ball      | Always fail             | /                   |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Dribble            | Player dribbles at least 3 meters with the ball  | Always success          | /                   |
+--------------------+--------------------------------------------------+-------------------------+---------------------+
| Goal kick          | Goal kick                                        | Always success          | /                   |
+--------------------+--------------------------------------------------+-------------------------+---------------------+


.. seealso:: 

  This `notebook`__ gives an example of the complete pipeline to download public
  StatsBomb data and convert it to the SPADL format.

__ https://github.com/ML-KULeuven/socceraction/blob/master/public-notebooks/1-load-and-convert-statsbomb-data.ipynb

Atomic-SPADL
============

Atomic-SPADL is an alternative version of SPADL which removes the "result"
attribute from SPADL and adds a few new action types. Each action is a now tuple
of the following eleven attributes:

:game_id:
  the ID of the game in which the action was performed
:period_id:
  the ID of the game period in which the action was performed
:seconds: 
  the action's start time
:player: 
  the player who performed the action
:team: 
  the player's team
:x: 
  the x location where the action started
:y: 
  the y location where the action started
:dx: 
  the distance covered by the action along the x-axis
:dy: 
  the distance covered by the action along the y-axis
:action_type: 
  the type of the action (e.g., pass, shot, dribble)
:bodypart: 
  the player's body part used for the action

In this representation, all actions are "atomic" in the sense that they are
always completed successfully without interruption. Consequently, while SPADL
treats a pass as one action consisting of both the initiation and receival of
the pass, Atomic-SPADL sees giving and receiving a pass as two separate
actions. Because not all passes successfully reach a teammate, Atomic-SPADL
introduces an "interception" action if the ball was intercepted by the other
team or an "out" event if the ball went out of play. Atomic-SPADL similarly
divides shots, freekicks, and corners into two separate actions. Practically,
the effect is that this representation helps to distinguish the contribution
of the player who initiates the action (e.g., gives the pass) and the player
who completes the action (e.g., receives the pass).


Example
-------

This is what Belgium’s second goal against England in the third place play-off
in the 2018 FIFA world cup looks like in the Atomic-SPADL format.

+---------+-----------+--------------+---------+-----------+-------+------+------+-------+-----------+---------------+
| game_id | period_id | time_seconds | team_id | player_id | x     | y    | dx   | dy    | type_name | bodypart_name |
+=========+===========+==============+=========+===========+=======+======+======+=======+===========+===============+
| 8657.0  | 2.0       | 2179.0       | 782.0   | 5642.0    | 37.1  | 44.8 | 0.0  | 0.0   | dribble   | foot          |
+---------+-----------+--------------+---------+-----------+-------+------+------+-------+-----------+---------------+
| 8657.0  | 2.0       | 2179.0       | 782.0   | 5642.0    | 37.1  | 44.8 | 16.8 | 3.4   | pass      | foot          |
+---------+-----------+--------------+---------+-----------+-------+------+------+-------+-----------+---------------+
| 8657.0  | 2.0       | 2180.0       | 782.0   | 3089.0    | 53.8  | 48.2 | 0.0  | 0.0   | receival  | foot          |
+---------+-----------+--------------+---------+-----------+-------+------+------+-------+-----------+---------------+
| 8657.0  | 2.0       | 2181.0       | 782.0   | 3089.0    | 53.8  | 48.2 | 16.8 | -6.0  | dribble   | foot          |
+---------+-----------+--------------+---------+-----------+-------+------+------+-------+-----------+---------------+
| 8657.0  | 2.0       | 2184.0       | 782.0   | 3089.0    | 70.6  | 42.2 | 16.8 | 6.9   | pass      | foot          |
+---------+-----------+--------------+---------+-----------+-------+------+------+-------+-----------+---------------+
| 8657.0  | 2.0       | 2184.5       | 782.0   | 3621.0    | 87.4  | 49.1 | 0.0  | 0.0   | receival  | foot          |
+---------+-----------+--------------+---------+-----------+-------+------+------+-------+-----------+---------------+
| 8657.0  | 2.0       | 2185.0       | 782.0   | 3621.0    | 87.4  | 49.1 | 10.6 | -10.3 | dribble   | foot          |
+---------+-----------+--------------+---------+-----------+-------+------+------+-------+-----------+---------------+
| 8657.0  | 2.0       | 2187.0       | 782.0   | 3621.0    | 97.9  | 38.7 | 7.1  | -1.4  | shot      | foot          |
+---------+-----------+--------------+---------+-----------+-------+------+------+-------+-----------+---------------+
| 8657.0  | 2.0       | 2187.0       | 782.0   | 3621.0    | 105.0 | 37.4 | 0.0  | 0.0   | goal      | foot          |
+---------+-----------+--------------+---------+-----------+-------+------+------+-------+-----------+---------------+

Here is the same phase visualized using the ``matplotsoccer`` package

.. figure:: ../eden_hazard_goal.png
   :alt: 

Definitions
-----------

In comparison to SPADL, Atomic-SPADL introduces eight new action types
that capture the outcome of an action.

+--------------------+----------------------------------------------------+
| Action type        | Description                                        |
+====================+====================================================+
| Receival           | Receiving a pass                                   |
+--------------------+----------------------------------------------------+
| Interception       | Interception of a pass by the other team           |
+--------------------+----------------------------------------------------+
| Out                | The ball went out of play                          |
+--------------------+----------------------------------------------------+
| Off-side           | The receiving player is off-side                   |
+--------------------+----------------------------------------------------+
| Goal               | A goal                                             |
+--------------------+----------------------------------------------------+
| Own goal           | An own goal                                        |
+--------------------+----------------------------------------------------+
| Yellow card        | A yellow card                                      |
+--------------------+----------------------------------------------------+
| Red card           | A red card                                         |
+--------------------+----------------------------------------------------+

Additionaly, two new action types are introduced that replace all types of
shots, freekicks, and corners in the original SPADL representation.

+--------------------+----------------------------------------------------+
| Action type        | Description                                        |
+====================+====================================================+
| Corner             | An action that combines crossed and short corners  |
+--------------------+----------------------------------------------------+
| Free-kick          | An action that combines short free-kicks,          |
|                    | crossed free-kicks and free-kick shots             |
+--------------------+----------------------------------------------------+


.. seealso:: 

  This `notebook`__ gives an example of the complete pipeline to download public
  StatsBomb data and convert it to the Atommic-SPADL format.

__ https://github.com/ML-KULeuven/socceraction/blob/master/public-notebooks/ATOMIC-1-load-and-convert-statsbomb-data.ipynb



Supported data providers
========================

This package currently supports converters for `Opta <https://www.optasports.com>`__, 
`Wyscout <https://www.wyscout.com>`__, and
`StatsBomb <https://www.statsbomb.com>`__ event stream data.
