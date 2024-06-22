import os
from urllib.error import HTTPError

import pytest
from socceraction.data.base import (
    _auth_remoteloadjson,
    _has_auth,
    _localloadjson,
    _remoteloadjson,
)


def test_load_json_from_url() -> None:
    url = "https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/15946.json"
    result = _remoteloadjson(url)
    assert isinstance(result, list)
    assert isinstance(result[0], dict)


def test_has_auth() -> None:
    assert not _has_auth({})
    assert not _has_auth({"user": "", "passwd": "test_passwd"})
    assert not _has_auth({"user": "test_user"})
    assert not _has_auth({"passwd": "test_passwd"})
    assert _has_auth({"user": "test_user", "passwd": "test_passwd"})


def test_load_json_from_url_with_auth() -> None:
    # use httpbin.org to test authentication
    user = "test_user"
    passwd = "test_passwd"
    url = f"http://httpbin.org/basic-auth/{user}/{passwd}"
    # add authentication header
    _auth_remoteloadjson(user, passwd)
    # the remote_load_json header should now use the authentication header
    try:
        result = _remoteloadjson(url)
        assert isinstance(result, dict)
        assert result.get("authenticated") is True
    except HTTPError as e:
        if e.code == 504:
            pytest.skip("httpbin.org is down or too slow")


def test_load_json_from_file() -> None:
    data_dir = os.path.join(os.path.dirname(__file__), os.pardir, "datasets", "statsbomb", "raw")
    result = _localloadjson(os.path.join(data_dir, "events", "15946.json"))
    assert isinstance(result, list)
    assert isinstance(result[0], dict)
