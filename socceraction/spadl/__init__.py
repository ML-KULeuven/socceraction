import socceraction.spadl.opta as op
import socceraction.spadl.statsbomb as sb
from socceraction.spadl.spadlcfg import *
import pandas as pd

optajson_to_optah5 = op.jsonfiles_to_h5
optah5_to_spadlh5 =  op.convert_to_spadl

statsbombjson_to_statsbombh5 = sb.jsonfiles_to_h5
statsbombh5_to_spadlh5 = sb.convert_to_spadl


def get_actions(spadlh5,game_id):
    actions = pd.read_hdf(spadlh5,f"actions/game_{game_id}")

    actiontypes = pd.read_hdf(spadlh5, "actiontypes")
    bodyparts = pd.read_hdf(spadlh5, "bodyparts")
    results = pd.read_hdf(spadlh5, "results")

    actions = (
        actions.merge(actiontypes)
        .merge(results)
        .merge(bodyparts)
        .merge(players,"left",on="player_id")
        .merge(teams,"left",on="team_id")
        .sort_values(["period_id", "time_seconds", "timestamp"])
        .reset_index(drop=True)
    )
    return actions