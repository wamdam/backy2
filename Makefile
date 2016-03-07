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

build: build/backy

build/backy: env
	$(PEX) . --cache-dir=$(PEXCACHE) --no-wheel -m backy2.scripts.backy:main -o build/backy

.PHONY : clean-build
clean-build:
	mkdir -p build
	rm build/backy || true
	rm -r $(PEXCACHE) || true

.PHONY : clean
clean: clean-build
	rm -r env || true

.PHONY : test
test: info
	$(PYTEST) -vv

.PHONY : smoketest
smoketest:
	$(PYTHON) smoketest.py

.PHONY : install
install: build
	cp build/backy /usr/local/sbin/backy
	mkdir -p /var/lib/backy
	if [ -f /etc/backy.cfg ]; then cp backy.cfg.dist /etc/backy.cfg.dist; fi
	if [ ! -f /etc/backy.cfg ]; then cp backy.cfg.dist /etc/backy.cfg; fi
