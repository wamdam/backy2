PYTHON=env/bin/python3
PIP=env/bin/pip
PYTEST=env/bin/py.test
PEX=env/bin/pex
PEXCACHE=build/.pex

CURRENT_VERSION := $(shell python3 setup.py --version)
GITHUB_ACCESS_TOKEN := $(shell cat .github-access-token)

all: build/benji.pex deb

.PHONY : deb
deb:
	fakeroot make -f debian/rules clean
	fakeroot make -f debian/rules binary
	mkdir -p dist
	ln -f ../benji_$(CURRENT_VERSION)_all.deb dist

env: setup.py
	virtualenv -p python3 env
	$(PYTHON) setup.py develop
	$(PIP) install pex==1.2.13
	$(PIP) install -r requirements_tests.txt
	$(PIP) install -r requirements_docs.txt

.PHONY : info
info:
	@python --version
	@virtualenv --version
	@pip --version
	@pip list

build/benji.pex: env $(wildcard src/benji/*.py) $(wildcard src/benji/**/*.py)
	mkdir -p build
	rm build/backy || true
	rm -r $(PEXCACHE) || true
	$(PEX) . --cache-dir=$(PEXCACHE) --no-wheel -m benji.scripts.backy:main -o build/benji.pex 'boto>=2.38.0' 'psycopg2>=2.6.1'

.PHONY : clean
clean:
	rm build/benji.pex || true
	fakeroot make -f debian/rules clean
	rm -r env || true

.PHONY : test
test: info
	$(PYTEST) -vv

.PHONY : smoketest
smoketest: env
	$(PYTHON) smoketest.py

.PHONY : release
release: env build/benji.pex deb
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
	# create release
	curl --data '{"tag_name": "v$(CURRENT_VERSION)", "target_commitish": "master", "name": "$(CURRENT_VERSION)", "body": "Release $(CURRENT_VERSION)", "draft": false, "prerelease": false}' https://api.github.com/repos/wamdam/benji/releases?access_token=$(GITHUB_ACCESS_TOKEN)
	# upload sdist, deb and pex release
	RELEASE_ID=$$(curl -sH "Authorization: token $(GITHUB_ACCESS_TOKEN)" https://api.github.com/repos/wamdam/benji/releases/tags/v$(CURRENT_VERSION) | grep -m 1 "id.:" | grep -w id | tr : = | tr -cd '=[[:alnum:]]' | cut -d '=' -f 2); \
	curl -i -H "Authorization: token $(GITHUB_ACCESS_TOKEN)" -H "Accept: application/vnd.github.manifold-preview" -H "Content-Type: application/octet-stream" --data-binary @dist/benji_$(CURRENT_VERSION)_all.deb https://uploads.github.com/repos/wamdam/benji/releases/$$RELEASE_ID/assets\?name\=benji_$(CURRENT_VERSION)_all.deb; \
	curl -i -H "Authorization: token $(GITHUB_ACCESS_TOKEN)" -H "Accept: application/vnd.github.manifold-preview" -H "Content-Type: application/octet-stream" --data-binary @build/benji.pex https://uploads.github.com/repos/wamdam/benji/releases/$$RELEASE_ID/assets\?name\=benji.pex; \
	curl -i -H "Authorization: token $(GITHUB_ACCESS_TOKEN)" -H "Accept: application/vnd.github.manifold-preview" -H "Content-Type: application/octet-stream" --data-binary @dist/benji-$(CURRENT_VERSION).tar.gz https://uploads.github.com/repos/wamdam/benji/releases/$$RELEASE_ID/assets\?name\=bacly2_$(CURRENT_VERSION).tar.gz

	# docs release
	@echo "--------------------------------------------------------------------------------"
	@echo "Releasing docs and website"
	cd docs && $(MAKE) html
	cd website && ./sync.sh

.PHONY : bbb
bbb:
