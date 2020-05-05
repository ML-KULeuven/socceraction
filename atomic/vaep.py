import pandas as pd


def _prev(x):
    prev_x = x.shift(1)
    prev_x[:1] = x.values[0]
    return prev_x


_samephase_nb = 15


def offensive_value(actions, scores, concedes):
    sameteam = _prev(actions.team_id) == actions.team_id
    prev_scores = _prev(scores) * sameteam + _prev(concedes) * (~sameteam)

    # if the previous action was too long ago, the odds of scoring are now 0
    #toolong_idx = (
    #    abs(actions.time_seconds - _prev(actions.time_seconds)) > _samephase_nb
    #)
    #prev_scores[toolong_idx] = 0

    # if the previous action was a goal, the odds of scoring are now 0
    prevgoal_idx = _prev(actions.type_name).isin(["goal","owngoal"])
    prev_scores[prevgoal_idx] = 0

    return scores - prev_scores


def defensive_value(actions, scores, concedes):
    sameteam = _prev(actions.team_id) == actions.team_id
    prev_concedes = _prev(concedes) * sameteam + _prev(scores) * (~sameteam)

    # if the previous action was too long ago, the odds of scoring are now 0
    #toolong_idx = (
    #    abs(actions.time_seconds - _prev(actions.time_seconds)) > _samephase_nb
    #)
    #prev_concedes[toolong_idx] = 0

    # if the previous action was a goal, the odds of conceding are now 0
    prevgoal_idx = _prev(actions.type_name).isin(["goal","owngoal"])
    prev_concedes[prevgoal_idx] = 0

    return -(concedes - prev_concedes)


def value(actions, Pscores, Pconcedes):
    v = pd.DataFrame()
    v["offensive_value"] = offensive_value(actions, Pscores, Pconcedes)
    v["defensive_value"] = defensive_value(actions, Pscores, Pconcedes)
    v["vaep_value"] = v["offensive_value"] + v["defensive_value"]
    return v
