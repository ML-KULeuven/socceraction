# Socceraction
Socceraction is a python package containing
- **SPADL** (Soccer Player Action Description Language): a unified and expressive language for on-the-ball player actions in soccer
- **VAEP** (Valuing Actions by Estimating Probabilities): a framework to value actions on their expected impact on the score line

## Installing and using this package

To install this package, simply do: `pip install socceraction`

The folder `public-notebooks` in the git repository provides a demo of the full pipeline from raw StatsBomb data to action values and player ratings using all available functions in the three subpackages:
```
socceraction.spadl
socceraction.classification
socceraction.vaep
```


## Why SPADL?
Processing existing event stream formats poses a number of challenges.
- _Inclusion of useless events._ For example, Opta event stream data includes "weather changes".
- _Gaps in the data._ For example, one actions ends at a specific location on the field and another action starts 5 seconds later on a completely different location on the field
- _Vendor specific terminology._ Opta, WyScout and StatsBomb all use their own terminology to describe events on the field.
- _Optional information._ All event stream data contains some optional information snippets per event. For example, a pass could have been an assist, low over the ground, offside, etc. The inclusion of these optional information snippets means that all event stream data is encoded in dynamic file format such as XML or JSON. This makes for a rich data source, but is also incredibly tricky to process.

SPADL is a language for describing player actions, as opposed to the formats by commercial vendors that describe events. The distinction is that actions are a subset of events that require a player to perform the action. For example, a passing event is an action, whereas an event signifying the end of the game is not an action. SPADL was designed to be _human-interpretable_, _simple_ and _complete_ to accurately define and describe actions on the pitch. Unlike all other event stream formats, we always store the same attributes for each action. Excluding optional information snippets enables us to store our data in a table and more easily apply automatic analysis tools.

This package currently supports converters for [Opta](https://www.optasports.com), [Wyscout](https://www.wyscout.com), and [StatsBomb](https://www.statsbomb.com) event stream data.

Here is an example of five actions in the SPADL format leading up to Belgium's second goal against England in the third place play-off in the 2018 FIFA world cup.


|   game_id |   period_id |   seconds | team    | player          |   start_x |   start_y |   end_x |   end_y | actiontype   | result   | bodypart   |
|-----------|-------------|-----------|---------|-----------------|-----------|-----------|---------|---------|--------------|----------|------------|
|      8657 |           2 |      2179 | Belgium | Axel Witsel     |      37.1 |      44.8 |    53.8 |    48.2 | pass         | success  | foot       |
|      8657 |           2 |      2181 | Belgium | Kevin De Bruyne |      53.8 |      48.2 |    70.6 |    42.2 | dribble      | success  | foot       |
|      8657 |           2 |      2184 | Belgium | Kevin De Bruyne |      70.6 |      42.2 |    87.4 |    49.1 | pass         | success  | foot       |
|      8657 |           2 |      2185 | Belgium | Eden Hazard     |      87.4 |      49.1 |    97.9 |    38.7 | dribble      | success  | foot       |
|      8657 |           2 |      2187 | Belgium | Eden Hazard     |      97.9 |      38.7 |   105   |    37.4 | shot         | success  | foot       |


Here is the same phase visualized using the `matplotsoccer` package
```
matplotsoccer.actions(
    location=actions[["start_x", "start_y", "end_x", "end_y"]],
    action_type=actions.type_name,
    team=actions.team_name,
    result= actions.result_name == "success",
    label=actions[["time_seconds", "type_name", "player_name", "team_name"]],
    labeltitle=["time","actiontype","player","team"],
    zoom=False
)
```
![](docs/eden_hazard_goal.png)

## Why VAEP?
Valuing actions is a key task in soccer analytics. Unfortunately this is a hard task because >99% actions do not directly affect the score. VAEP is a framework to value actions on their expected impact on the score line. The intuition is that all good actions should aim to (a) increase the chance of scoring a goal in the short-term future and/or (b) decrease the chance of conceding a goal in the short-term future.

## Info

For more information about SPADL and VAEP, read our SIGKDD paper **"Actions Speak Louder Than Goals: Valuing Player Actions in Soccer"** available on ACM (https://dl.acm.org/citation.cfm?doid=3292500.3330758) and Arxiv (https://arxiv.org/abs/1802.07127).

If you make use of this package or the ideas in our paper, please use the following citation:
```
@inproceedings{Decroos:2019:ASL:3292500.3330758,
 author = {Decroos, Tom and Bransen, Lotte and Van Haaren, Jan and Davis, Jesse},
 title = {Actions Speak Louder Than Goals: Valuing Player Actions in Soccer},
 booktitle = {Proceedings of the 25th ACM SIGKDD International Conference on Knowledge Discovery \&\#38; Data Mining},
 series = {KDD '19},
 year = {2019},
 isbn = {978-1-4503-6201-6},
 location = {Anchorage, AK, USA},
 pages = {1851--1861},
 numpages = {11},
 url = {http://doi.acm.org/10.1145/3292500.3330758},
 doi = {10.1145/3292500.3330758},
 acmid = {3330758},
 publisher = {ACM},
 address = {New York, NY, USA},
 keywords = {event stream data, probabilistic classification, soccer match data, sports analytics, valuing actions},
} 
```