# -*- coding: utf-8 -*-
"""Configuration of the Atomic-SPADL language.

Attributes
----------
field_length : float
    The length of a pitch (in meters).
field_width : float
    The width of a pitch (in meters).
bodyparts : list(str)
    The bodyparts used in the Atomic-SPADL language.
actiontypes : list(str)
    The action types used in the Atomic-SPADL language.

"""
import pandas as pd

import socceraction.spadl.config as _spadl

field_length = _spadl.field_length
field_width = _spadl.field_width

bodyparts = _spadl.bodyparts
bodyparts_df = _spadl.bodyparts_df

actiontypes = _spadl.actiontypes + [
    'receival',
    'interception',
    'out',
    'offside',
    'goal',
    'owngoal',
    'yellow_card',
    'red_card',
    'corner',
    'freekick',
]


def actiontypes_df() -> pd.DataFrame:
    """Return a dataframe with the type id and type name of each Atomic-SPADL action type.

    Returns
    -------
    pd.DataFrame
        The 'type_id' and 'type_name' of each Atomic-SPADL action type.
    """
    return pd.DataFrame(list(enumerate(actiontypes)), columns=['type_id', 'type_name'])
