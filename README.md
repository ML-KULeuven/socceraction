<div align="center">
	<img src="https://socceraction.readthedocs.io/en/latest/_static/logo_white.png" height="200">
	<p>
	<b>Convert soccer event stream data to the SPADL format<br/>and value on-the-ball player actions.</b>
	</p>
	<br/>

[![PyPi](https://img.shields.io/pypi/v/socceraction.svg)](https://pypi.org/project/socceraction)
[![Python Version: 3.7.1+](https://img.shields.io/badge/Python-3.7.1+-blue.svg)](https://pypi.org/project/socceraction)
[![Downloads](https://img.shields.io/pypi/dm/socceraction.svg)](https://pypistats.org/packages/socceraction)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://en.wikipedia.org/wiki/MIT_License)

[![Build Status](https://img.shields.io/github/workflow/status/ML-KULeuven/socceraction/CI?label=build&logo=github)](https://github.com/ML-KULeuven/socceraction/actions?workflow=CI)
[![Read the Docs](https://img.shields.io/readthedocs/socceraction/latest.svg?label=Read%20the%20Docs)](https://socceraction.readthedocs.io)
[![Code coverage](https://codecov.io/gh/ML-KULeuven/socceraction/branch/master/graph/badge.svg)](https://codecov.io/gh/ML-KULeuven/socceraction)

<br/>
<br/>
</div>

Socceraction is a Python package for objectively quantifying the impact of the individual actions performed by soccer players using event stream data. The general idea is to assign a value to each on-the-ball action based on the action's impact on the game outcome, while accounting for the context in which the action happened. The video below gives a quick two-minute introduction to action values.

<div align="center">

https://user-images.githubusercontent.com/2175271/136857714-1d2c8706-7f2f-449d-818f-0e67fbb75400.mp4

</div>

## Features

Socceraction contains the following components:

- Loaders for StatsBomb, Opta, Wyscout, StatsPerform and WhoScored data that enable **loading events and corresponding metadata** as Pandas DataFrames using a unified data model. [Read more »](https://socceraction.readthedocs.io/en/latest/modules/data.html)
- Converters for event stream data to the **SPADL** and **atomic-SPADL** formats, which are unified and expressive languages for on-the-ball player actions. [Read more »](https://socceraction.readthedocs.io/en/latest/documentation/SPADL.html)
- An implementation of the **xT** framework to value ball-progressing actions using a possession-based Markov model. [Read more »](https://socceraction.readthedocs.io/en/latest/documentation/xT.html)
- An implementation of the **VAEP** and **Atomic-VAEP** frameworks to value actions on their expected impact on the score line. [Read more »](https://socceraction.readthedocs.io/en/latest/documentation/VAEP.html)

## Installation / Getting started

The recommended way to install `socceraction` is to simply use pip. The latest version officially supports Python 3.7.1 - 3.9.

```sh
$ pip install socceraction
```

The folder [`public-notebooks`](https://github.com/ML-KULeuven/socceraction/tree/master/public-notebooks) provides a demo of the full pipeline from raw StatsBomb event stream data to action values and player ratings. More detailed installation/usage instructions can be found in the [Documentation](https://socceraction.readthedocs.io/en/latest/).

## Contributing

All contributions, bug reports, bug fixes, documentation improvements, enhancements, and ideas are welcome. However, be aware that socceraction is not actively developed. It's primary use is to enable reproducibility of our research. If you believe there is a feature missing, feel free to raise a feature request, but please do be aware that the overwhelming likelihood is that your feature request will not be accepted.
To learn more on how to contribute, see the [Contributor Guide](https://socceraction.readthedocs.io/en/latest/development/developer_guide.html).

## Research

If you make use of this package in your research, please consider citing the following papers:

- Decroos, Tom, Lotte Bransen, Jan Van Haaren, and Jesse Davis. **Actions speak louder than goals: Valuing player actions in soccer.** In Proceedings of the 25th ACM SIGKDD International Conference on Knowledge Discovery & Data Mining, pp. 1851-1861. 2019. <br/>[ [pdf](http://doi.acm.org/10.1145/3292500.3330758) | [bibtex](https://github.com/ML-KULeuven/socceraction/blob/master/docs/_static/decroos19.bibtex) ]

- Maaike Van Roy, Pieter Robberechts, Tom Decroos, and Jesse Davis. **Valuing on-the-ball actions in soccer: a critical comparison of XT and VAEP.** In Proceedings of the AAAI-20 Workshop on Artifical Intelligence in Team Sports. AI in Team Sports Organising Committee, 2020. <br/>[ [pdf](https://limo.libis.be/primo-explore/fulldisplay?docid=LIRIAS2913207&context=L&vid=KULeuven&search_scope=ALL_CONTENT&tab=all_content_tab&lang=en_US) | [bibtex](https://github.com/ML-KULeuven/socceraction/blob/master/docs/_static/vanroy20.bibtex) ]

The Expected Threat (xT) framework was originally introduced by Karun Singh on his [blog](https://karun.in/blog/expected-threat.html) in 2019.

## License

Distributed under the terms of the [MIT license](https://opensource.org/licenses/MIT),
socceraction is free and open source software. Although not strictly required, we appreciate it if you include a link to this repo or cite our research in your work if you make use of socceraction.
