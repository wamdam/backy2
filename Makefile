PYTHON=env/bin/python3
PEX=env/bin/pex
PYTEST=env/bin/py.test
PEXCACHE=build/.pex

all: build

env:
	virtualenv -p python3 env
	$(PYTHON) setup.py develop

.PHONY : fastenv
fastenv:
	$(PYTHON) setup.py develop

.PHONY : info
info:
	@python --version
	@virtualenv --version
	@pip --version
	@pip list

build: clean-build env
	$(PEX) . --cache-dir=$(PEXCACHE) --no-wheel -m backy2.scripts.backy:main -o build/backy

.PHONY : clean-build
clean-build:
	mkdir -p build
	rm build/backy 2>/dev/null || true
	rm -r $(PEXCACHE) 2>/dev/null || true

.PHONY : clean
clean: clean-build
	rm -r env 2>/dev/null || true

.PHONY : test
test: info
	$(PYTEST) -vv

.PHONY : smoketest
smoketest:
	$(PYTHON) smoketest.py

.PHONY : install
install: build
	cp build/backy /usr/local/bin/backy
