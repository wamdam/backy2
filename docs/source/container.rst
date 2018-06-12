.. include:: global.rst.inc

Containerised Benji
===================

Images
------

benji-rook
~~~~~~~~~~

This image is based on the `Rook Toolbox <https://rook.io/docs/rook/master/toolbox.html>`_
image and additionally includes Benji of course and all of its dependencies.
``/usr/local/bin/toolbox.sh`` is still run in the background to update and
create ``/etc/ceph/ceph.conf`` so that a direct access to the Ceph RBD images
is possible. It also includes ``kubectl`` which is used to find RBD volumes
to include in the backup from PersistentVolumeClaim resources.

A container from this image can be parametrised with three enviroment variables:

+------------------+--------------------------------+----------------------------------------------+
| Name             | Default                        | Description                                  |
+==================+================================+==============================================+
| BACKUP_SCHEDULE  | 0 33 0 * * *                   | Backup interval as a Go cron expression      |
+------------------+--------------------------------+----------------------------------------------+
| BACKUP_RETENTION | latest3,hours24,days30,months3 | Retention policy like with ``benji enforce`` |
+------------------+--------------------------------+----------------------------------------------+
| BACKUP_SELECTOR  | nomatch==matchnot              | Kubernetes label selector applied to PVCs    |
+------------------+--------------------------------+----------------------------------------------+

The BACKUP_SCHEDULE determines how often the actual backup scripts is called.
This is a `Go cron expression <https://godoc.org/github.com/robfig/cron>`_.

The backup scripts first  searches for PersistemtVolumeClaims matching the
BACKUP_SELECTOR. By default there should be no matching PVCs, so you must
specify a different BACKUP_SELECTOR to get any backups. You can either use
an existing label or you can augment your PVCs with a new one. If you
specify an empty string as the BACKUP_SELECTOR all PVCs will be backed up.

.. TIP:: See https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
for possible ways to construct your selector.

After each volume backup ``benji enforce`` is called for the volume with
the retention policy specified in BACKUP_RETENTION. Currently the same
retention policy is applied to all PVCs.

The last step is to call ``benji cleanup`` to remove blocks on the
data backend and free the space of deleted *versions*.

The backup script uses Ceph's and Benji's differential backup features if
possible. Normally only the initial backup is a full backup. RBD snapshots
are generated with a prefix of *b-*.

Helm Charts
-----------

Helm charts are the preferred way to deploy Benji's Docker images.

benji-rook
~~~~~~~~~~

Benji includes a Helm chart to use the Docker image of the same name. It consists
of a Deployment and supporting resources and assumes that you have RBAC in place.
The deployment is composed a two containers: One running the benji-docker image
and another one running a `Prometheus <https://prometheus.io/>`_
`pushgateway <https://github.com/prometheus/pushgateway>`_ where the first container
pushes its metrics. These can be scraped by a Prometheus server and the Pod generated
by the Deployment has annotations so that it can be detected automatically.

.. NOTE:: The deployed resources create a service account which has the right to *get*,
    *list* and *watch* all PersistentVolume and PersistentVolumeClaim resources in all
    namespaces.
