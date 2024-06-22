"""Type aliases."""
from typing import Any, Callable, Union, TypeAlias

from pandera.typing import DataFrame, Series

from socceraction.atomic.spadl.schema import AtomicSPADLSchema
from socceraction.spadl.schema import SPADLSchema

SPADLActions : TypeAlias = DataFrame[SPADLSchema]
AtomicSPADLActions : TypeAlias = DataFrame[AtomicSPADLSchema]
Actions : TypeAlias = Union[SPADLActions, AtomicSPADLActions]
GameStates : TypeAlias = list[Actions]
Features : TypeAlias = DataFrame[Any]
GameStatesFeatureTransfomer : TypeAlias = Callable[[GameStates], Features]
Mask : TypeAlias = Series[bool]
