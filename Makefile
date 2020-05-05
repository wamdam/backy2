PYTHON=env/bin/python3
SPHINX_MULTIVERSION=env/bin/sphinx-multiversion
PIP=env/bin/pip
PYTEST=env/bin/py.test

CURRENT_VERSION := $(shell python3 setup.py --version)
GITHUB_ACCESS_TOKEN := $(shell cat .github-access-token)

# Use bash or source won't work
SHELL := /bin/bash

all: docs deb

.PHONY : deb
deb:
	fakeroot make -f debian/rules clean
	fakeroot make -f debian/rules binary
	mkdir -p dist
	ln -f ../backy2_$(CURRENT_VERSION)_all.deb dist

env: setup.py
	python3 -mvenv env
	$(PYTHON) setup.py develop
	$(PIP) install -r requirements_tests.txt
	$(PIP) install -r requirements_docs.txt

.PHONY : info
info:
	@python --version
	@virtualenv --version
	@pip --version
	@pip list

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

.PHONY : docs
docs: env
	source env/bin/activate && cd docs && make clean && make html
	#source env/bin/activate && ${SPHINX_MULTIVERSION} docs/source docs/build/html
	#cp docs/source/_static/index.html docs/build/html

.PHONY : release
release: env docs deb
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
	curl --data '{"tag_name": "v$(CURRENT_VERSION)", "target_commitish": "master", "name": "$(CURRENT_VERSION)", "body": "Release $(CURRENT_VERSION)", "draft": false, "prerelease": false}' https://api.github.com/repos/wamdam/backy2/releases?access_token=$(GITHUB_ACCESS_TOKEN)
	RELEASE_ID=$$(curl -sH "Authorization: token $(GITHUB_ACCESS_TOKEN)" https://api.github.com/repos/wamdam/backy2/releases/tags/v$(CURRENT_VERSION) | grep -m 1 "id.:" | grep -w id | tr : = | tr -cd '=[[:alnum:]]' | cut -d '=' -f 2); \
	curl -i -H "Authorization: token $(GITHUB_ACCESS_TOKEN)" -H "Accept: application/vnd.github.manifold-preview" -H "Content-Type: application/octet-stream" --data-binary @dist/backy2_$(CURRENT_VERSION)_all.deb https://uploads.github.com/repos/wamdam/backy2/releases/$$RELEASE_ID/assets\?name\=backy2_$(CURRENT_VERSION)_all.deb; \
	curl -i -H "Authorization: token $(GITHUB_ACCESS_TOKEN)" -H "Accept: application/vnd.github.manifold-preview" -H "Content-Type: application/octet-stream" --data-binary @dist/backy2-$(CURRENT_VERSION).tar.gz https://uploads.github.com/repos/wamdam/backy2/releases/$$RELEASE_ID/assets\?name\=bacly2_$(CURRENT_VERSION).tar.gz

	# docs release
	@echo "--------------------------------------------------------------------------------"
	@echo "Releasing docs and website"
	#cd docs && $(MAKE) html
	cd website && ./sync.sh

.PHONY : bbb
bbb:
