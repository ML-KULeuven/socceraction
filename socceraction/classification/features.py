import socceraction.spadl as spadl
import pandas as pd
import numpy as np


_spadlcolumns =["game_id","period_id",
                    "time_seconds","timestamp",
                    "team_id","player_id","start_x","start_y",
                    "end_x","end_y","result_id","result_name",
                    "bodypart_id","bodypart_name","type_id","type_name"]
_dummy_actions = pd.DataFrame(np.zeros((10,len(_spadlcolumns))),columns = _spadlcolumns)
for c in _spadlcolumns:
    if "name" in c:
        _dummy_actions[c] = _dummy_actions[c].astype(str)

def feature_column_names(fs,nb_prev_actions=3):
    gs = gamestates(_dummy_actions,nb_prev_actions)
    return list(pd.concat([f(gs) for f in fs],axis=1).columns)

def gamestates(actions, nb_prev_actions=3):
    """This function take a dataframe <actions> and outputs gamestates.
     Each gamestate is represented as the <nb_prev_actions> previous actions.

     The list of gamestates is internally represented as a list of actions dataframes [a_0,a_1,..] 
     where each row in the a_i dataframe contains the previous action of 
     the action in the same row in the a_i-1 dataframe.
     """
    states = [actions]
    for i in range(1, nb_prev_actions):
        prev_actions = actions.copy().shift(i, fill_value=0)
        prev_actions.loc[: i - 1, :] = pd.concat([actions[:1]] * i, ignore_index=True)
        states.append(prev_actions)
    return states


def play_left_to_right(gamestates, home_team_id):
    a0 = gamestates[0]
    away_idx = a0.team_id != home_team_id
    for actions in gamestates:
        for col in ["start_x", "end_x"]:
            actions.loc[away_idx, col] = (
                spadl.spadl_length - actions[away_idx][col].values
            )
        for col in ["start_y", "end_y"]:
            actions.loc[away_idx, col] = (
                spadl.spadl_width - actions[away_idx][col].values
            )
    return gamestates


def simple(actionfn):
    "Function decorator to apply actionfeatures to gamestates"

    def wrapper(gamestates):
        if not isinstance(gamestates, (list,)):
            gamestates = [gamestates]
        X = []
        for i, a in enumerate(gamestates):
            Xi = actionfn(a)
            Xi.columns = [c + "_a" + str(i) for c in Xi.columns]
            X.append(Xi)
        return pd.concat(X, axis=1)

    return wrapper


# SIMPLE FEATURES


@simple
def actiontype(actions):
    return actions[["type_id"]]


@simple
def actiontype_onehot(actions):
    X = pd.DataFrame()
    for type_name in spadl.actiontypes:
        col = "type_" + type_name
        X[col] = actions["type_name"] == type_name
    return X


@simple
def result(actions):
    return actions[["result_id"]]


@simple
def result_onehot(actions):
    X = pd.DataFrame()
    for result_name in spadl.results:
        col = "result_" + result_name
        X[col] = actions["result_name"] == result_name
    return X

@simple
def actiontype_result_onehot(actions):
    res = result_onehot(actions)
    tys = actiontype_onehot(actions)
    df = pd.DataFrame()
    for tyscol in list(tys.columns):
        for rescol in list(res.columns):
            df[tyscol + "_" + rescol] = tys[tyscol] & res[rescol]
    return df 



@simple
def bodypart(actions):
    return actions[["bodypart_id"]]


@simple
def bodypart_onehot(actions):
    X = pd.DataFrame()
    for bodypart_name in spadl.bodyparts:
        col = "bodypart_" + bodypart_name
        X[col] = actions["bodypart_name"] == bodypart_name
    return X


@simple
def time(actions):
    timedf = actions[["period_id", "time_seconds"]].copy()
    timedf["time_seconds_overall"] = (
        (timedf.period_id - 1) * 45 * 60
    ) + timedf.time_seconds
    return timedf


@simple
def startlocation(actions):
    return actions[["start_x", "start_y"]]

@simple
def endlocation(actions):
    return actions[["end_x", "end_y"]]


_goal_x = spadl.spadl_length
_goal_y = spadl.spadl_width / 2


@simple
def startpolar(actions):
    polardf = pd.DataFrame()
    dx = _goal_x - actions["start_x"]
    dy = abs(_goal_y - actions["start_y"])
    polardf["start_dist_to_goal"] = np.sqrt(dx ** 2 + dy ** 2)
    polardf["start_tan_angle_to_goal"] = np.divide(dx, dy, where=dy != 0)
    return polardf

@simple
def endpolar(actions):
    polardf = pd.DataFrame()
    dx = _goal_x - actions["end_x"]
    dy = abs(_goal_y - actions["end_y"])
    polardf["end_dist_to_goal"] = np.sqrt(dx ** 2 + dy ** 2)
    polardf["end_tan_angle_to_goal"] = np.divide(dx, dy, where=dy != 0)
    return polardf


@simple
def movement(actions):
    mov = pd.DataFrame()
    mov["dx"] = actions.end_x - actions.start_x
    mov["dy"] = actions.end_y - actions.start_y
    mov["movement"] = np.sqrt(mov.dx ** 2 + mov.dy ** 2)
    return mov


# STATE FEATURES


def team(gamestates):
    a0 = gamestates[0]
    teamdf = pd.DataFrame()
    for i, a in enumerate(gamestates[1:]):
        teamdf["team_" + (str(i + 1))] = a.team_id == a0.team_id
    return teamdf


def time_delta(gamestates):
    a0 = gamestates[0]
    dt = pd.DataFrame()
    for i, a in enumerate(gamestates[1:]):
        dt["time_delta_" + (str(i + 1))] = a0.time_seconds - a.time_seconds
    return dt


def space_delta(gamestates):
    a0 = gamestates[0]
    spaced = pd.DataFrame()
    for i, a in enumerate(gamestates[1:]):
        dx = a.end_x - a0.start_x
        spaced["dx_a0" + (str(i + 1))] = dx
        dy = a.end_y - a0.start_y
        spaced["dy_a0" + (str(i + 1))] = dy
        spaced["mov_a0" + (str(i + 1))] = np.sqrt(dx ** 2 + dy ** 2)
    return spaced


# CONTEXT FEATURES


def goalscore(gamestates):
    """
    This function determines the nr of goals scored by each team after the 
    action
    """
    actions = gamestates[0]
    teamA = actions["team_id"].values[0]
    goals = actions["type_name"].str.contains("shot") & (
        actions["result_id"] == spadl.results.index("success")
    )
    owngoals = actions["type_name"].str.contains("shot") & (
        actions["result_id"] == spadl.results.index("owngoal")
    )
    teamisA = actions["team_id"] == teamA
    teamisB = ~teamisA
    goalsteamA = (goals & teamisA) | (owngoals & teamisB)
    goalsteamB = (goals & teamisB) | (owngoals & teamisA)
    goalscoreteamA = goalsteamA.cumsum() - goalsteamA
    goalscoreteamB = goalsteamB.cumsum() - goalsteamB

    scoredf = pd.DataFrame()
    scoredf["goalscore_team"] = (goalscoreteamA * teamisA) + (goalscoreteamB * teamisB)
    scoredf["goalscore_opponent"] = (goalscoreteamB * teamisA) + (
        goalscoreteamA * teamisB
    )
    scoredf["goalscore_diff"] = (
        scoredf["goalscore_team"] - scoredf["goalscore_opponent"]
    )
    return scoredf