FROM python:3.6-stretch AS build

ENV CEPH_VERSION 12.2.5

# If you change VENV_PATH you also need to change the path to activate in scripts/backup.sh.
ENV VENV_PATH /benji

RUN apt-get update && \
	apt-get upgrade -y -q && \
	apt-get install -y -q --force-yes --no-install-recommends software-properties-common apt-transport-https && \
	curl -sSL https://download.ceph.com/keys/release.asc -o- | apt-key add - && \
	apt-add-repository 'deb https://download.ceph.com/debian-luminous/ stretch main' && \
	apt-get update && \
	apt-get install  -y -q --force-yes --no-install-recommends git gcc && \
	apt-get install -y -q --force-yes --no-install-recommends librados2 librbd1 librados-dev librbd-dev

RUN python3.6 -m venv $VENV_PATH && \
	ln -s /usr/bin/python3.6-config $VENV_PATH/bin/python-config && \
	. $VENV_PATH/bin/activate && \
	pip install --upgrade pip && \
	sed -i "s|self.run_command(\['clone', '-q', url, dest\])|self.run_command(['clone', '--depth=1', '--branch', rev if rev else 'master', '-q', url, dest])|" $(python -c 'import site; print(site.getsitepackages()[0])')/pip/_internal/vcs/git.py

RUN . $VENV_PATH/bin/activate && \
	pip install git+https://github.com/kurtbrose/aes_keywrap && \
	pip install git+https://github.com/elemental-lf/sparsebitfield && \
	pip install boto3 b2 pycryptodome diskcache cython zstandard && \
	pip install "git+https://github.com/ceph/ceph@v$CEPH_VERSION#subdirectory=src/pybind/rados" && \
	pip install "git+https://github.com/ceph/ceph@v$CEPH_VERSION#subdirectory=src/pybind/rbd"

ADD . /benji-source/
RUN . $VENV_PATH/bin/activate && \
	pip install /benji-source/

FROM python:3.6-slim-stretch AS runtime

ENV SCRIPTS_PATH /scripts

RUN apt-get update && \
	apt-get upgrade -y -q && \
	apt-get install -y -q --force-yes --no-install-recommends software-properties-common apt-transport-https curl gnupg2 && \
	curl -sSL https://download.ceph.com/keys/release.asc -o- | apt-key add - && \
	apt-add-repository 'deb https://download.ceph.com/debian-luminous/ stretch main' && \
	apt-get update && \
	apt-get install -y -q --force-yes --no-install-recommends librados2 librbd1 ceph-base && \
	apt-get install -y -q --force-yes --no-install-recommends bash-completion joe jq && \
	rm -rf /var/lib/apt/lists/*

COPY --from=build $VENV_PATH/ $VENV_PATH/
COPY scripts/ $SCRIPTS_PATH/
COPY etc/benji-minimal.yaml /etc/benji/benji.yaml
COPY images/common/benji-wrapper /usr/bin/benji
COPY images/common/bashrc /root/.bashrc

RUN curl -o /usr/bin/kubectl -sSL https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl && \
	chmod a+x /usr/bin/kubectl /usr/bin/benji

ENTRYPOINT ["/bin/bash"]
CMD ["-i"]
