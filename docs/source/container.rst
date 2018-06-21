.. include:: global.rst.inc
.. _container:

Containerised Benji
===================

Images
------

benji
~~~~~

This image is based on the Debian 9 (Slim Stretch) variant of
`library/python <https://hub.docker.com/_/python/>`_. It includes
Benji and its dependencies. Ceph support as well kubectl for Kubernetes
integration are also present.

The Benji configuration should be put into ``/etc/benji/benji.yaml``.
Either by inheriting from this image and overwriting it or by mounting
it directly into the container. By default a minimal test configuration
is provided by the image.

The default Docker entry point is just ``/bin/bash``.

The first use case for this image is to simply test Benji::

    docker run --interactive --tty --rm elementalnet/benji

After that you can directly proceed with step 1 of the instructions
in section :ref:`quickstart`.

The second use case would be to get some real work done without
directly installing Benji on the host. A series of scripts to
facilitate the calling of Benji are provided under ``/scripts``
inside of the container. They can also be found in the ``scripts``
directory of the source distribution. For an example of how to
use these scripts please see ``images/benji-rook/scripts/backup.sh``.

benji-rook
~~~~~~~~~~

This image is based on the `Rook Toolbox <https://rook.io/docs/rook/master/toolbox.html>`_
image and additionally includes Benji of course and all of its dependencies.
``/usr/local/bin/toolbox.sh`` is still run in the background to update and
create ``/etc/ceph/ceph.conf`` so that a direct access to the Ceph RBD images
is possible. It also includes ``kubectl`` which is used to find RBD volumes
to include in the backup from PersistentVolumeClaim resources.

A container from this image can be parametrised with three enviroment variables:

+------------------------------------+--------------------------------+----------------------------------------------+
| Name                               | Default                        | Description                                  |
+====================================+================================+==============================================+
| BACKUP_SCHEDULE                    | 0 33 0 * * *                   | Backup interval as a Go cron expression      |
+------------------------------------+--------------------------------+----------------------------------------------+
| BACKUP_RETENTION                   | latest3,hours24,days30,months3 | Retention policy like with ``benji enforce`` |
+------------------------------------+--------------------------------+----------------------------------------------+
| BACKUP_SELECTOR                    | nomatch==matchnot              | Kubernetes label selector applied to PVCs    |
+------------------------------------+--------------------------------+----------------------------------------------+
| DEEP_SCRUBBING_ENABLED             |                              1 | Enable bulk deep scrubbing                   |
+------------------------------------+--------------------------------+----------------------------------------------+
| DEEP_SCRUBBING_VERSIONS_PERCENTAGE |                              6 | Percentage of versions to check              |
+------------------------------------+--------------------------------+----------------------------------------------+
| DEEP_SCRUBBING_BLOCKS_PERCENTAGE   |                             50 | Percentage of blocks to check                |
+------------------------------------+--------------------------------+----------------------------------------------+
| SCRUBBING_ENABLED                  |                              0 | Enable bulk scrubbing                        |
+------------------------------------+--------------------------------+----------------------------------------------+
| SCRUBBING_VERSIONS_PERCENTAGE      |                              6 | Percentage of versions to check              |
+------------------------------------+--------------------------------+----------------------------------------------+
| SCRUBBING_BLOCKS_PERCENTAGE        |                             50 | Percentage of blocks to check                |
+------------------------------------+--------------------------------+----------------------------------------------+
| PROM_PUSH_GATEWAY                  | :9091                          | Address of Prometheus push gateway           |
+------------------------------------+--------------------------------+----------------------------------------------+

The BACKUP_SCHEDULE environment variable determines how often the actual backup
scripts is called. This is a `Go cron expression <https://godoc.org/github.com/robfig/cron>`_.
There currently is no support for more advanced scheduling options.

The backup scripts first  searches for PersistemtVolumeClaims matching the
BACKUP_SELECTOR. By default there should be no matching PVCs, so you must
specify a different BACKUP_SELECTOR to get any backups. You can either use
an existing label or you can augment your PVCs with a new one. If you
specify an empty string as the BACKUP_SELECTOR all PVCs will be backed up.

.. TIP:: See https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
    for possible ways to construct your selector.

After each volume backup ``benji enforce`` is called for the volume with
the retention policy specified in BACKUP_RETENTION. Currently the same
retention policy is applied to all PVCs. BACKUP_RETENTION and BACKUP_SCHEDULE
have to be considered together.

The next step is to call ``benji cleanup`` to remove blocks on the
data backend and free the space of deleted *versions*.

The two last steps are deep scrubbing and scrubbing of *versions* if enabled.
A random sample of *versions* is picked according to the configured
DEEP_SCRUBBING_VERSIONS_PERCENTAGE (or SCRUBBING_VERSIONS_PERCENTAGE
respectively). Of these *versions* a randomly sampled selection of blocks
will be checked. The number of blocks checked from each *version* is
determined by DEEP_SCRUBBING_BLOCKS_PERCENTAGE (or SCRUBBING_BLOCKS_PERCENTAGE
in the case of metadata only scrubbing).

At the end a number of different `Prometheus <https://prometheus.io/>`_ is pushed
to the configured `pushgateway <https://github.com/prometheus/pushgateway>`_.

The backup script uses Ceph's and Benji's differential backup features if
possible. Normally only the initial backup is a full backup. RBD snapshots
names are generated with a prefix of *b-*.

Helm Charts
-----------

Helm charts are the preferred way to deploy Benji's Docker images.

benji-rook
~~~~~~~~~~

Benji includes a Helm chart to use the Docker image of the same name. It consists
of a Deployment and supporting resources and assumes that you have RBAC in place.
The deployment is composed a two containers: One running the benji-rook Docker image
and another one running a `Prometheus <https://prometheus.io/>`_
`pushgateway <https://github.com/prometheus/pushgateway>`_. These can be scraped
by a Prometheus server and the Pod generated by the Deployment has annotations
so that it can be detected automatically::

      annotations:
        prometheus.io/port: "{{ .Values.pushgateway.port }}"
        prometheus.io/scrape: "true"

.. NOTE:: The deployed resources create a service account which has the right to *get*,
    *list* and *watch* all PersistentVolume and PersistentVolumeClaim resources in all
    namespaces.
