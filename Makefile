.PHONY: init test lint pretty notebooks precommit_install bump_major bump_minor bump_patch clean

BIN = .venv/bin/
CODE = socceraction

init:
	python3 -m venv .venv
	poetry install

tests/datasets/statsbomb/spadl-WorldCup-2018.h5:
	$(BIN)python tests/datasets/download.py statsbomb

tests/datasets/wyscout_public/spadl-WorldCup-2018.h5:
	$(BIN)python tests/datasets/download.py wyscout

test: tests/datasets/statsbomb/spadl-WorldCup-2018.h5 tests/datasets/wyscout_public/spadl-WorldCup-2018.h5
	nox -rs tests -- $(args)

mypy:
	nox -rs mypy -- $(args)

lint:
	nox -rs lint -- $(args)

pretty:
	nox -rs precommit -- $(args)

notebooks:
	$(BIN)python -m nbconvert --execute --inplace --config=default.json public-notebooks/*.ipynb

precommit_install:
	nox -rs pre-commit -- install

bump_major:
	$(BIN)bumpversion major

bump_minor:
	$(BIN)bumpversion minor

bump_patch:
	$(BIN)bumpversion patch

clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf tests/datasets/wyscout_public
	rm -rf tests/datasets/statsbomb
