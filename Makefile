.PHONY: init test lint pretty precommit_install bump_major bump_minor bump_patch clean

BIN = .venv/bin/
CODE = socceraction

init:
	python3 -m venv .venv
	$(BIN)pip install -e .
	$(BIN)pip install -r requirements_dev.txt

tests/data/statsbomb/spadl-WorldCup-2018.h5:
	$(BIN)python tests/data/download.py statsbomb

tests/data/wyscout_public/spadl-WorldCup-2018.h5:
	$(BIN)python tests/data/download.py wyscout

test: tests/data/statsbomb/spadl-WorldCup-2018.h5 tests/data/wyscout_public/spadl-WorldCup-2018.h5
	$(BIN)pytest --verbosity=2 --showlocals --strict --log-level=DEBUG $(args)

lint:
	$(BIN)flake8 --jobs 4 --statistics --show-source $(CODE) tests
	$(BIN)pylint --rcfile=setup.cfg --exit-zero $(CODE)
	$(BIN)pydocstyle socceraction
	$(BIN)mypy $(CODE) tests
	$(BIN)black --target-version py36 --skip-string-normalization --line-length=99 --check $(CODE) tests
	$(BIN)pytest --dead-fixtures --dup-fixtures

pretty:
	$(BIN)isort $(CODE) tests
	$(BIN)black --target-version py36 --skip-string-normalization --line-length=99 $(CODE) tests
	$(BIN)unify --in-place --recursive $(CODE) tests

precommit_install:
	echo '#!/bin/sh\nmake lint test\n' > .git/hooks/pre-commit
	chmod +x .git/hooks/pre-commit

bump_major:
	$(BIN)bumpversion major

bump_minor:
	$(BIN)bumpversion minor

bump_patch:
	$(BIN)bumpversion patch

clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf tests/data/wyscout_public
	rm -rf tests/data/statsbomb
