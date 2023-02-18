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

Socceraction implements two versions of this action-oriented data format: :ref:`SPADL`
and :ref:`Atomic-SPADL`.

.. toctree::
  :hidden:
  :maxdepth: 1

  spadl
  atomic_spadl
