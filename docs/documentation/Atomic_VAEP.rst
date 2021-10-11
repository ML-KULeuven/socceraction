Atomic-VAEP
-----------

When building models to value actions, a heavy point of debate is how to
handle the results of actions. In other words, should our model make
a distinction between a failed and a successful pass or not? On the one hand,
an action should be valued on all its properties, and whether or not the
action was successful (e.g., did a pass receive a teammate, was a shot
converted into a goal) plays a crucial role in how useful the action was. That
is, if you want to measure a player's contribution during a match, successful
actions are important. This is the viewpoint of SPADL and VAEP.

On the other hand, including the result of an action intertwines the
contribution of the player who started the action (e.g., provides the pass)
and the player who completes it (e.g., receives the pass). Perhaps a pass was
not successful because of its recipient's poor touch or because he was not
paying attention. It would seem unfair to penalize the player who provided the
pass in such a circumstance. Hence, it can be useful to generalize over
possible results of an action to arrive at an action's "expected value".

The combination of Atomic-SPADL and VAEP accomodates this alternative viewpoint.
Atomic-SPADL removes the "result" attribute from SPADL and adds a few new
action and event types. This affects the features that can be computed to
represent each game state. By default, Atomic-VAEP uses the following
features to encoded a gamestate :math:`S_i = \{a_{i-2}, a_{i−1}, a_i\}`:

+-----------------------------------------------------------+------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Transformer                                               | Feature                | Description                                                                                                                                                |
+===========================================================+========================+============================================================================================================================================================+
| :func:`~socceraction.atomic.vaep.features.actiontype`     | actiontype(_onehot)_ai | The (one-hot encoding) of the action's type.                                                                                                               |
+-----------------------------------------------------------+------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
| :func:`~socceraction.atomic.vaep.features.bodypart`       | actiontype(_onehot)_ai | The (one-hot encoding) of the bodypart used to perform the action.                                                                                         |
+-----------------------------------------------------------+------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
| :func:`~socceraction.atomic.vaep.features.time`           | time_ai                | Time in the match the action takes place, recorded to the second.                                                                                          |
+-----------------------------------------------------------+------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
| :func:`~socceraction.atomic.vaep.features.team`           | team_ai                | Boolean indicating whether the team that had possesion in action :math:`a_{i-2}` still has possession in the current action.                               |
+-----------------------------------------------------------+------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
| :func:`~socceraction.atomic.vaep.features.time_delta`     | time_delta_i           | Seconds elapsed between :math:`a_{i-2}` and the current action.                                                                                            |
+-----------------------------------------------------------+------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
| :func:`~socceraction.atomic.vaep.features.location`       | x_ai                   | The x pitch coordinate of the action.                                                                                                                      |
|                                                           +------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
|                                                           | y_ai                   | The y pitch coordinate of the action.                                                                                                                      |
+-----------------------------------------------------------+------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
| :func:`~socceraction.atomic.vaep.features.polar`          | dist_to_goal_ai        | The distance to the center of the goal.                                                                                                                    |
|                                                           +------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
|                                                           | angle_to_goal_ai       | The angle between the start location and center of the goal.                                                                                               |
+-----------------------------------------------------------+------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
| :func:`~socceraction.atomic.vaep.features.movement_polar` | mov_d_ai               | The distance covered by the action.                                                                                                                        |
|                                                           +------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
|                                                           | mov_angle_ai           | The direction in which the action was executed (relative to the top left of the field).                                                                    |
+-----------------------------------------------------------+------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
| :func:`~socceraction.atomic.vaep.features.direction`      | dx_ai                  | Direction of the action, expressed as the x-component of the unit vector.                                                                                  |
|                                                           +------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
|                                                           | dy_ai                  | Direction of the action, expressed as the y-component of the unit vector.                                                                                  |
+-----------------------------------------------------------+------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
| :func:`~socceraction.atomic.vaep.features.goalscore`      | goalscore_team         | The number of goals scored by the team executing the action.                                                                                               |
|                                                           +------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
|                                                           | goalscore_opponent     | The number of goals scored by the other team.                                                                                                              |
|                                                           +------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+
|                                                           | goalscore_diff         | The goal difference between both teams.                                                                                                                    |
+-----------------------------------------------------------+------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------+

The computation of the labels and the VAEP formula are similar to the standard
VAEP model.

Empirically, we have noticed two benefits of using the Atomic-SPADL
representation. First, the standard SPADL representation tends to assign shots
a value that is the difference between the shot’s true outcome and its xG
score. Hence, goals or a number of misses, particularly for players who do not
take a lot of shots can have an outsized effect on their VAEP score. In
contrast, Atomic-SPADL assigns shots a value closer to their xG score, which
often better matches domain experts’ intuitions on action values.

Second, Atomic-SPADL leads to more robust action values and player ratings.
A good rating system should capture the true quality of all players. Although
some fluctuations in performances are possible across games, over the course
of a season a few outstanding performances (possibly stemming from a big
portion of luck) should not dramatically alter an assessment of a player. In
our prior work comparing VAEP to xT, one advantage of xT was that it produced
more stable ratings. Using Atomic-SPADL helps alleviate this weakness.

.. seealso::

  A set of notebooks illustrates the complete pipeline to train and
  apply an Atomic-VAEP model:

  1. `compute features and labels`__
  2. `estimate scoring and conceding probabilities`__
  3. `compute VAEP values and top players`__

__ https://github.com/ML-KULeuven/socceraction/blob/master/public-notebooks/ATOMIC-2-compute-features-and-labels.ipynb
__ https://github.com/ML-KULeuven/socceraction/blob/master/public-notebooks/ATOMIC-3-estimate-scoring-and-conceding-probabilities.ipynb
__ https://github.com/ML-KULeuven/socceraction/blob/master/public-notebooks/ATOMIC-4-compute-vaep-values-and-top-players.ipynb
