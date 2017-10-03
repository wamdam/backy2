PYTHON=env/bin/python3
PIP=env/bin/pip
PYTEST=env/bin/py.test
PEX=env/bin/pex
PEXCACHE=build/.pex

CURRENT_VERSION := $(shell python3 setup.py --version)
GITHUB_ACCESS_TOKEN := $(shell cat .github-access-token)

all: build/backy2.pex deb

.PHONY : deb
deb:
	fakeroot make -f debian/rules binary

env: setup.py
	virtualenv -p python3 env
	$(PYTHON) setup.py develop
	$(PIP) install pex==1.1.0
	$(PIP) install -r requirements_tests.txt
	$(PIP) install -r requirements_docs.txt

.PHONY : info
info:
	@python --version
	@virtualenv --version
	@pip --version
	@pip list

build/backy2.pex: env $(wildcard src/backy2/*.py) $(wildcard src/backy2/**/*.py)
	mkdir -p build
	rm build/backy || true
	rm -r $(PEXCACHE) || true
	$(PEX) . --cache-dir=$(PEXCACHE) --no-wheel -m backy2.scripts.backy:main -o build/backy2.pex 'boto>=2.38.0' 'psycopg2>=2.6.1'

.PHONY : clean
clean:
	rm build/backy2.pex || true
	fakeroot make -f debian/rules clean
	rm -r env || true

.PHONY : test
test: info
	$(PYTEST) -vv

.PHONY : smoketest
smoketest: env
	$(PYTHON) smoketest.py

.PHONY : release
release: env
	@echo ""
	@echo "--------------------------------------------------------------------------------"
	@echo Releasing Version $(CURRENT_VERSION)

	# pypi release
	@echo "--------------------------------------------------------------------------------"
	@echo Pypi...
	$(PYTHON) setup.py sdist upload

	# github release
	@echo "--------------------------------------------------------------------------------"
	@echo "Releasing at github"
	git push github
	curl --data '{"tag_name": "v$(CURRENT_VERSION)", "target_commitish": "master", "name": "$(CURRENT_VERSION)", "body": "Release $(CURRENT_VERSION)", "draft": true, "prerelease": true}' https://api.github.com/repos/wamdam/backy2/releases?access_token=$(GITHUB_ACCESS_TOKEN)

	# docs release
	@echo "--------------------------------------------------------------------------------"
	@echo "Releasing docs and website"
	cd docs && $(MAKE) html
	cd website && ./sync.sh

