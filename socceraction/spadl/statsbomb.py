import pandas as pd
import tqdm
import ujson as json
import os

def jsonfiles_to_h5(datafolder, h5file):
    add_competitions(os.path.join(datafolder,"competitions.json"),h5file)
    add_matches(os.path.join(datafolder,"matches/"),h5file)
    add_players_and_teams(os.path.join(datafolder,"lineups/"),h5file)
    add_events(os.path.join(datafolder,"events/"),h5file)

def add_competitions(competitions_url,h5file):
    with open(competitions_url,'r') as fh:
        competitions = json.load(fh)
    pd.DataFrame(competitions).to_hdf(h5file,"competitions")

def add_matches(matches_url,h5file):
    matches = []
    for competition_file in get_jsonfiles(matches_url):
        with open(competition_file,'r') as fh:
            matches += json.load(fh)
    pd.DataFrame([flatten(m) for m in matches]).to_hdf(h5file,"matches")


def add_players_and_teams(lineups_url,h5file):
    lineups = []
    players = []
    for competition_file in get_jsonfiles(lineups_url):
        with open(competition_file,'r') as fh:
            lineups += json.load(fh)
            for lineup in lineups:
                players += [flatten_id(p) for p in lineup["lineup"]]
    players = pd.DataFrame(players)
    players.drop_duplicates("player_id").reset_index(drop=True).to_hdf(h5file,"players")
    teams = pd.DataFrame(lineups)[["team_id","team_name"]]
    teams.drop_duplicates("team_id").reset_index(drop=True).to_hdf(h5file,"teams")

def get_match_id(url):
    return url.split("/")[-1].replace(".json","")

def add_events(events_url,h5file):
    for events_file in tqdm.tqdm(get_jsonfiles(events_url),desc = f"converting events files to {h5file}"):
        with open(events_file,'r') as fh:
            events = json.load(fh)
        eventsdf = pd.DataFrame([flatten_id(e) for e in events])
        match_id =  get_match_id(events_file)
        eventsdf["match_id"] = match_id
        eventsdf.to_hdf(h5file,f"events/match_{match_id}")

def get_jsonfiles(folder):
    return [
        os.path.join(folder, f) for f in os.listdir(folder) if ".json" in f
    ]


def flatten(d):
    newd = {}
    for k,v in d.items():
        if isinstance(v,dict):
            newd = {**newd,**flatten(v)}
        else:
            newd[k] = v
    return newd
        
def flatten_id(d):
    newd = {}
    extra = {}
    for k,v in d.items():
        if isinstance(v,dict):
            if len(v) == 2 and "id" in v and "name" in v:
                newd[k + "_id"] = v["id"]
                newd[k + "_name"] = v["name"]
            else:
                extra[k] = v
        else:
            newd[k] = v
    newd["extra"] = extra
    return newd

