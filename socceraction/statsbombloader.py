import pandas as pd
import requests
import json
import os

"""
This is a temporary module until statsbombpy* becomes compatible with socceraction
*https://github.com/statsbomb/statsbombpy
"""


def remoteloadjson(path):
    return requests.get(path).json()


def localloadjson(path):
    with open(path, "rt", encoding="utf-8") as fh:
        return json.load(fh)


_free_open_data = "https://raw.githubusercontent.com/statsbomb/open-data/master/data/"


class StatsBombLoader:
    """
    Load Statsbomb data either from a remote location
    (e.g., "https://raw.githubusercontent.com/statsbomb/open-data/master/data/")
    or from a local folder.
    """

    def __init__(self, root=_free_open_data, getter="remote"):
        """
        Initalize the StatsBombLoader

        :param root: root-path of the data
        :param getter: "remote" or "local"
        """
        self.root = root

        if getter == "remote":
            self.get = remoteloadjson
        elif getter == "local":
            self.get = localloadjson
        else:
            raise Exception("invalid getter specified")

    def competitions(self):
        return pd.DataFrame(self.get(os.path.join(self.root, "competitions.json")))

    def matches(self, competition_id, season_id):
        return pd.DataFrame(
            _flatten(m)
            for m in self.get(
                os.path.join(self.root, f"matches/{competition_id}/{season_id}.json")
            )
        )

    def _lineups(self, match_id):
        lineups = self.get(os.path.join(self.root, f"lineups/{match_id}.json"))
        return lineups

    def teams(self, match_id):
        return pd.DataFrame(self._lineups(match_id))[["team_id", "team_name"]]

    def players(self, match_id):
        return pd.DataFrame(
            _flatten_id(p)
            for lineup in self._lineups(match_id)
            for p in lineup["lineup"]
        )

    def events(self, match_id):
        eventsdf = pd.DataFrame(
            _flatten_id(e)
            for e in self.get(os.path.join(self.root, f"events/{match_id}.json"))
        )
        eventsdf["match_id"] = match_id
        return eventsdf


def _flatten_id(d):
    newd = {}
    extra = {}
    for k, v in d.items():
        if isinstance(v, dict):
            if len(v) == 2 and "id" in v and "name" in v:
                newd[k + "_id"] = v["id"]
                newd[k + "_name"] = v["name"]
            else:
                extra[k] = v
        else:
            newd[k] = v
    newd["extra"] = extra
    return newd


def _flatten(d):
    newd = {}
    for k, v in d.items():
        if isinstance(v, dict):
            newd = {**newd, **_flatten(v)}
        else:
            newd[k] = v
    return newd
