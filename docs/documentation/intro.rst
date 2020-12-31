Quickstart
===========

Eager to get started valuing some soccer actions? This page gives a quick introduction in how to get started with socceraction.

Installation
------------

First, make sure that socceraction is installed:

.. code-block:: console

   $ pip install socceraction

For detailed instructions and other installation options, check out our
detailed :doc:`installation instructions <install>`.

Data
----

First of all, you will need some data. Luckily, both `StatsBomb <https://github.com/statsbomb/open-data>`_ and
`Wyscout <https://www.nature.com/articles/s41597-019-0247-7>`_ provide a small freely available dataset.
In this short introduction, we will work with Statsbomb's dataset of the 2018
World Cup. With the code below, you can load all data from the tournament.


.. code-block:: python

   import pandas as pd
   import socceraction.spadl.statsbomb as statsbomb

   # Set up the StatsBomb data loader
   SBL = statsbomb.StatsBombLoader()

   # Get data from the 2018 World Cup as pandas dataframes
   matches = SBL.matches(competition_id=43, season_id=3).set_index("match_id")
   teams, players, events = [], [], {}
   for match_id in matches.index:
       teams.append(SBL.teams(match_id))
       players.append(SBL.players(match_id))
       events[match_id] = SBL.events(match_id)
   teams = pd.concat(teams).drop_duplicates("team_id").set_index("team_id")
   players = pd.concat(players).drop_duplicates("player_id").set_index("player_id")


The event stream format is not well-suited for data analysis: some of the
recorded information is irrelavant for valuing actions, each vendor uses their
own custom format and definitions, and the events are stored as unstructured
JSON objects. Therefore, socceraction uses the :doc:`SPADL format <SPADL>` for describing
actions on the pitch. With the code below, you can convert the events to
actions.

.. code-block:: python

   actions = {}
   for match_id in matches.index:
       home_team_id = matches.at[match_id, "home_team_id"]
       actions[match_id] = statsbomb.convert_to_actions(events[match_id], home_team_id)

With the `matplotsoccer package <https://github.com/TomDecroos/matplotsoccer>`_, you can try plotting some of these
actions:

.. code-block:: python

    import matplotsoccer as mps

    df_actions = actions[8657].loc[2297:2202]
    mps.actions(
        location=df_actions[["start_x", "start_y", "end_x", "end_y"]],
        action_type=df_actions.type_name,
        team=df_actions.team_name,
        result=df_actions.result_name == "success",
        label=df_actions[["time_seconds", "type_name", "player_name", "team_name"]],
        labeltitle=["time","actiontype","player","team"],
        zoom=False
    )

.. figure:: ../eden_hazard_goal.png
   :alt: 


Valuing actions with xT
-----------------------

Socceraction implements two frameworks for valuing actions: xT and VAEP. The
expected threat or xT model is a possession-based model. That is, it divides
a match into possessions or phases, which are sequences of consecutive
on-the-ball actions where the same team possesses the ball. A Markov model is
used to model how progressing the ball in these possession sequences changes
a team's chances of procucing a goal-scoring attempt. Therefore, xT overlays
a M × N grid on the pitch in order to divide it into M · N zones, each
corresponding to a transient state in the Markov model. Each zone z is then
assigned a value xT(z) that reflects how threatening teams are at that
location, in terms of scoring. The code below allows you to train such
a Markov model.

.. code-block:: python

    import socceraction.spadl.config as spadl
    import socceraction.xthreat as xthreat
    import socceraction.vaep.features as fs

    for match_id, df_actions in actions.items()
        home_team_id = matches.at[match_id, "home_team_id"]
        df_actions = spadl.add_names(game_actions)
        [gamestates] = fs.play_left_to_right([actions], home_team_id)

    ## Train model
    xTModel = xthreat.ExpectedThreat(l=16, w=12)
    xTModel.fit(gamestates)


Subsequently, the model can be used to value actions that successfully move
the ball between two zones by computing the difference between the threat
value on the start and end location of each action:

.. code-block:: python

    ## Predict
    # xT should only be used to value actions that move the ball·
    # and also keep the current team in possession of the ball
    mov_actions = xthreat.get_successful_move_actions(actions)
    mov_actions["xT_value"] = xTModel.predict(mov_actions)



Valuing actions with VAEP
-------------------------

VAEP goes beyond the possession-based approach of xT by trying to value
a broader set of actions and by taking the action and game context into
account. Therfore, VAEP frames the problem of quantifying a soccer player’s
contributions within a match as a binary classification task and rates each
action by estimating its effect on the short-term probabilities that a team
will both score or concede. That is, VAEP quanitifies the effect of an action :math:`a_i`
that moves the game from state :math:`S_{i−1} = \{a_{i-n}, \ldots, a_{i−1}\}` to state 
:math:`S_i = \{a_{i-n+1}, . . . , a_{i−1}, a_i\}`, where each game state is
represented by the :math:`n` previous actions. Then each game state is
represented using a set of features and assigned two labels. A first label
that defines whether the team  in possession scored a goal in the next
:math:`k` actions; a second label that defines whether the team  in possession
conceded a goal in the next :math:`k` actions. 

This allows to train two
classifiers: one that predicts the probability that a team will score in the
next :math:`k` actions from the current game state (:math:`P_{scores}`) and
one that predicts the probability that a team will concede in the
next :math:`k` actions from the current game state (:math:`P_{concedes}`).

.. code-block:: python

    from socceraction.vaep import VAEP

    model = VAEP()
    # comppute features and labels 
    features = pd.concat([model.compute_features(match, actions[match_id]) 
                          for match_id, match in matches.iterrows()])
    labels = pd.concat([model.compute_labels(match, actions[match_id]) 
                          for match_id, match in matches.iterrows()])
    # fit the model
    model.fit(features, labels)

Given these probabilites, VAEP estimates the risk-reward trade-off of an
action as the sum of the offensive value :math:`\Delta
P_\textrm{score}(a_{i})` (i.e., how much did the action increase the probability of
scoring) and defensive value :math:`- \Delta P_\textrm{concede}(a_{i})` (i.e., how
much did the action decrease the probability of conceding) of the action:
:math:`\textrm{VAEP}(a_i) = \Delta P_\textrm{score}(a_{i}) - \Delta P_\textrm{concede}(a_{i})`.


.. code-block:: python

    # rate a game
    match = matches.loc[7584]
    match_actions = actions[7584]
    ratings = model.rate(match, match_actions)


-----------------------

Ready for more? Check out the detailed documentation about the 
:doc:`data representation <SPADL>` and :doc:`action value frameworks <valuing_actions>`.

