<div align="center">
	<img src="docs/_static/logo_white.png" height="200">
	<p>
		<b>Convert soccer event stream data to the SPADL format<br/>and value on-the-ball player actions</b>
	</p>
	<br>
	<br>
</div>

[![pypi](https://badge.fury.io/py/socceraction.svg)](https://pypi.org/project/socceraction)
[![Python: 3.6+](https://img.shields.io/badge/Python-3.6+-blue.svg)](https://pypi.org/project/socceraction)
[![Downloads](https://img.shields.io/pypi/dm/socceraction.svg)](https://pypistats.org/packages/socceraction)
[![Build Status](https://travis-ci.org/{{cookiecutter.github_username}}/socceraction.svg?branch=master)](https://travis-ci.org/{{cookiecutter.github_username}}/socceraction)
[![Code coverage](https://codecov.io/gh/{{cookiecutter.github_username}}/socceraction/branch/master/graph/badge.svg)](https://codecov.io/gh/{{cookiecutter.github_username}}/socceraction)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://en.wikipedia.org/wiki/MIT_License)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

This is a Python package for objectively quantifying the impact of the individual actions performed by soccer players using event stream data. It contains the following components:

- Converters for event stream data to the **SPADL** and **atomic-SPADL** formats, which are unified and expressive languages for on-the-ball player actions.   [Read more »](docs/documentation/SPADL.rst)
- An implementation of the **VAEP** and **Atomic-VAEP** frameworks to value actions on their expected impact on the score line.  [Read more »](docs/documentation/VAEP.rst)
- An implementation of the **xT** framework to value ball-progressing actions using a possession-based Markov model.  [Read more »](docs/documentation/xT.rst)

<br/>
<p align="center">
  <img src="docs/actions_bra-bel.png" width="650" title="Example Brazil-Belgium">
</p>

## Installation / Getting started

The recommended way to install `socceraction` is to simply use pip:

```sh
$ pip install socceraction
```

`socceraction` officially supports Python 3.6---3.8.

The folder [`public-notebooks`](public-notebooks) provides a demo of the full pipeline from raw StatsBomb data to action values and player ratings. More detailed installation/usage instructions can be found in [the documentation](TODO).

## Research

For more information about SPADL and VAEP, read our SIGKDD paper **"Actions Speak Louder Than Goals: Valuing Player Actions in Soccer"** available on ACM (https://dl.acm.org/citation.cfm?doid=3292500.3330758) and Arxiv (https://arxiv.org/abs/1802.07127).

For more information about xT, read Karun Singh's blog post: https://karun.in/blog/expected-threat.html

If you make use of this package or the ideas in our paper, please use the following citation:
```
@inproceedings{Decroos2019actions,
 author = {Decroos, Tom and Bransen, Lotte and Van Haaren, Jan and Davis, Jesse},
 title = {Actions Speak Louder Than Goals: Valuing Player Actions in Soccer},
 booktitle = {Proceedings of the 25th ACM SIGKDD International Conference on Knowledge Discovery and Data Mining},
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
