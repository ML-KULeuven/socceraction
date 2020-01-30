import pandas as pd # type: ignore
import socceraction.spadl.config as spadl


def scores(actions : pd.DataFrame, nr_actions : int =10) -> pd.DataFrame:
    """
    This function determines whether a goal was scored by the team possessing 
    the ball within the next x actions
    """
    # merging goals, owngoals and team_ids

    goals = actions["type_name"].str.contains("shot") & (
        actions["result_id"] == spadl.results.index("success")
    )
    owngoals = actions["type_name"].str.contains("shot") & (
        actions["result_id"] == spadl.results.index("owngoal")
    )
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


def concedes(actions: pd.DataFrame, nr_actions=10) -> pd.DataFrame:
    """
    This function determines whether a goal was scored by the team not 
    possessing the ball within the next x actions
    """
    # merging goals,owngoals and team_ids
    goals = actions["type_name"].str.contains("shot") & (
        actions["result_id"] == spadl.results.index("success")
    )
    owngoals = actions["type_name"].str.contains("shot") & (
        actions["result_id"] == spadl.results.index("owngoal")
    )
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


def goal_from_shot(actions : pd.DataFrame) -> pd.DataFrame:
    goals = actions["type_name"].str.contains("shot") & (
        actions["result_id"] == spadl.results.index("success")
    )

    return pd.DataFrame(goals, columns=["goal_from_shot"])

