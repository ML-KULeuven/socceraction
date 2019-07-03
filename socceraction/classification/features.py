import socceraction.spadl.spadl as spadl
import pandas as pd
import numpy as np


def gamestates(actions, nb_prev_actions):
    states = [actions]
    for i in range(1, nb_prev_actions):
        prev_actions = actions.copy().shift(i, fill_value=0)
        prev_actions.loc[: i - 1, :] = pd.concat([actions[:1]]*i,ignore_index=True)
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
    return actions.result_id


@simple
def result_onehot(actions):
    X = pd.DataFrame()
    for result_name in spadl.results:
        col = "result_" + result_name
        X[col] = actions["result_name"] == result_name
    return X


@simple
def bodypart(actions):
    return actions.bodypart_id


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
def location(actions):
    return actions[["start_x", "start_y", "end_x", "end_y"]]


goal_x = spadl.spadl_length
goal_y = spadl.spadl_width / 2

@simple
def polar(actions):
    polardf = pd.DataFrame()
    for part in ["start", "end"]:
        dx = goal_x - actions["%s_x" % part]
        dy = abs(goal_y - actions["%s_y" % part])
        polardf["%s_dist_to_goal" % part] = np.sqrt(dx ** 2 + dy ** 2)
        polardf["%s_tan_angle_to_goal" % part] = np.divide(dx, dy, where=dy != 0)
    return polardf

@simple
def movement(actions):
    mov = pd.DataFrame()
    mov["dx"] = actions.end_x - actions.start_x
    mov["dy"] =  actions.end_y - actions.start_y
    mov["movement"] = np.sqrt(mov.dx**2 + mov.dy**2)
    return mov


# state features

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
        spaced["mov_a0" + (str(i + 1))] = np.sqrt(dx**2 + dy**2)
    return spaced


# context features

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
    scoredf["goalscore_team"] = (goalscoreteamA * teamisA) + (
        goalscoreteamB * teamisB
    )
    scoredf["goalscore_opponent"] = (goalscoreteamB * teamisA) + (
        goalscoreteamA * teamisB
    )
    scoredf["goalscore_diff"] = scoredf["goalscore_team"] - scoredf["goalscore_opponent"]
    return scoredf