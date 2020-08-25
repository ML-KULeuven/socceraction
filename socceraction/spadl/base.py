import json
from abc import ABC, abstractmethod
from typing import Dict, List

import pandas as pd  # type: ignore
import requests
import ssl; ssl._create_default_https_context = ssl._create_unverified_context


def _remoteloadjson(path: str) -> List[Dict]:
    return requests.get(path).json()


def _localloadjson(path: str) -> List[Dict]:
    with open(path, "rt", encoding="utf-8") as fh:
        return json.load(fh)


class EventDataLoader(ABC):
    """
    Load event data either from a remote location or from a local folder.
    """

    def __init__(self, root, getter):
        """
        Initalize the EventDataLoader

        :param root: root-path of the data
        :param getter: "remote" or "local"
        """
        self.root = root

        if getter == "remote":
            self.get = _remoteloadjson
        elif getter == "local":
            self.get = _localloadjson
        else:
            raise Exception("Invalid getter specified")

    @abstractmethod
    def competitions(self) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def matches(self, competition_id: int, season_id: int) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def teams(self, match_id: int) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def players(self, match_id: int) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def events(self, match_id: int) -> pd.DataFrame:
        raise NotImplementedError
