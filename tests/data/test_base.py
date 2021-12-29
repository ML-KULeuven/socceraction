import os

from socceraction.data.base import _localloadjson, _remoteloadjson


def test_load_json_from_url() -> None:
    url = "https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/15946.json"
    result = _remoteloadjson(url)
    assert isinstance(result, list)
    assert isinstance(result[0], dict)


def test_load_json_from_file() -> None:
    data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "datasets", "statsbomb", "raw")
    result = _localloadjson(os.path.join(data_dir, "events", "15946.json"))
    assert isinstance(result, list)
    assert isinstance(result[0], dict)
