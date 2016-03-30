PYTHON=env/bin/python3
PYTEST=env/bin/py.test

all: deb

.PHONY : deb
deb:
	fakeroot make -f debian/rules binary

env: setup.py
	virtualenv -p python3 env
	$(PYTHON) setup.py develop

.PHONY : info
info:
	@python --version
	@virtualenv --version
	@pip --version
	@pip list

build/backy: env $(wildcard src/backy2/*.py) $(wildcard src/backy2/**/*.py)
	mkdir -p build
	rm build/backy || true
	rm -r $(PEXCACHE) || true
	$(PEX) . --cache-dir=$(PEXCACHE) --no-wheel -m backy2.scripts.backy:main -o build/backy

.PHONY : clean
clean:
	fakeroot make -f debian/rules clean
	rm -r env || true

.PHONY : test
test: info
	$(PYTEST) -vv

.PHONY : smoketest
smoketest: env
	$(PYTHON) smoketest.py
