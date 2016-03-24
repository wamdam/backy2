PYTHON=env/bin/python3
PEX=env/bin/pex
PYTEST=env/bin/py.test
PEXCACHE=build/.pex

all: build/backy

env: setup.py
	virtualenv -p python3 env
	$(PYTHON) setup.py develop

#.PHONY : fastenv
#fastenv: setup.py
#	$(PYTHON) setup.py develop

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
	mkdir -p build
	rm build/backy || true
	rm -r $(PEXCACHE) || true
	rm -r env || true

.PHONY : test
test: info
	$(PYTEST) -vv

.PHONY : smoketest
smoketest: env
	$(PYTHON) smoketest.py

.PHONY : install
install: build
	cp build/backy /usr/local/sbin/backy
	mkdir -p /var/lib/backy
	if [ -f /etc/backy.cfg ]; then cp backy.cfg.dist /etc/backy.cfg.dist; fi
	if [ ! -f /etc/backy.cfg ]; then cp backy.cfg.dist /etc/backy.cfg; fi
