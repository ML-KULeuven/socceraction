import pandas as pd
import socceraction.spadl.config as spadl
import atomic.atomic_spadl as atomic


def scores(actions, nr_actions=10):
    """
    This function determines whether a goal was scored by the team possessing 
    the ball within the next x actions
    """
    # merging goals, owngoals and team_ids
    goals = actions["type_id"] == atomic.atomic_actiontypes.index("goal")
    owngoals = actions["type_id"] == atomic.atomic_actiontypes.index("owngoal")
    y = pd.concat([goals, owngoals, actions["team_id"]], axis=1)
    y.columns = ["goal", "owngoal", "team_id"]

    # adding future results
    for i in range(1, nr_actions):
        for c in ["team_id", "goal", "owngoal"]:
            shifted = y[c].shift(-i)
            shifted[-i:] = y[c][len(y) - 1]
            y["%s+%d" % (c, i)] = shifted

    res = y["goal"]
    for i in range(1, nr_actions):
        gi = y["goal+%d" % i] & (y["team_id+%d" % i] == y["team_id"])
        ogi = y["owngoal+%d" % i] & (y["team_id+%d" % i] != y["team_id"])
        res = res | gi | ogi

    return pd.DataFrame(res, columns=["scores"])


def concedes(actions, nr_actions=10):
    """
    This function determines whether a goal was scored by the team not 
    possessing the ball within the next x actions
    """
    # merging goals, owngoals and team_ids
    goals = actions["type_id"] == atomic.atomic_actiontypes.index("goal")
    owngoals = actions["type_id"] == atomic.atomic_actiontypes.index("owngoal")
    y = pd.concat([goals, owngoals, actions["team_id"]], axis=1)
    y.columns = ["goal", "owngoal", "team_id"]

    # adding future results
    for i in range(1, nr_actions):
        for c in ["team_id", "goal", "owngoal"]:
            shifted = y[c].shift(-i)
            shifted[-i:] = y[c][len(y) - 1]
            y["%s+%d" % (c, i)] = shifted

    res = y["owngoal"]
    for i in range(1, nr_actions):
        gi = y["goal+%d" % i] & (y["team_id+%d" % i] != y["team_id"])
        ogi = y["owngoal+%d" % i] & (y["team_id+%d" % i] == y["team_id"])
        res = res | gi | ogi

    return pd.DataFrame(res, columns=["concedes"])


def goal_from_shot(actions):
    next_actions = actions.shift(-1)
    goals = ((actions["type_id"] == atomic.atomic_actiontypes.index("shot"))
            & (actions["type_id"] == atomic.atomic_actiontypes.index("goal")))

    return pd.DataFrame(goals, columns=["goal"])

