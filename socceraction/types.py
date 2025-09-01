"""Type aliases."""

from typing import Any, Union

import numpy as np
import numpy.typing as npt
from pandera.typing import DataFrame

from socceraction.atomic.spadl.schema import AtomicSPADLSchema
from socceraction.spadl.schema import SPADLSchema

SPADLActions = DataFrame[SPADLSchema]
AtomicSPADLActions = DataFrame[AtomicSPADLSchema]
Actions = Union[SPADLActions, AtomicSPADLActions]
GameStates = list[Actions]
Features = DataFrame[Any]
Mask = npt.NDArray[np.bool_]
