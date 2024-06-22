import socceraction.spadl.utils as spu
import socceraction.vaep.labels as lab
from pandera.typing import DataFrame
from socceraction.spadl import SPADLSchema


def test_scores(spadl_actions: DataFrame[SPADLSchema]) -> None:
    nr_actions = 10
    spadl_actions = spu.add_names(spadl_actions)
    scores = lab.scores(spadl_actions, nr_actions)
    assert len(scores) == len(spadl_actions)


def test_conceds(spadl_actions: DataFrame[SPADLSchema]) -> None:
    nr_actions = 10
    spadl_actions = spu.add_names(spadl_actions)
    concedes = lab.concedes(spadl_actions, nr_actions)
    assert len(concedes) == len(spadl_actions)
