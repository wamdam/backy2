.. include:: global.rst.inc

Benji Backup Configuration
==========================

Benji only needs to be configured once.

.. _config_file:

benji.yaml
----------

This is the an example configuration which includes all possible
configuration options:

.. literalinclude:: ../../etc/benji.yaml

Custom Configuration File
-------------------------

Benji will per default search the following locations for configuration files:

* /etc/benji.yaml
* /etc/benji/benji.yaml
* ~/.benji.yaml
* ~/benji.yaml

In case multiple of these configurations exist, only the first match is read.

In order to explicitly pass a configuration file, use the ``-c`` (or
``--configfile``) parameter::

  benji -c ./my_test_vm.cfg ls

Multiple Instance Installations
-------------------------------

You can run Benji multiple times on different machines or in different
containers simultaneously with matching configurations (i.e.  accessing the
same database and data backend).  The configurations will have to match and
this is the responsibility of the user as this isn't checked by Benji.  Be
careful to shutdown all instances before making configuration changes that
could affect other instances (like adding a encryption key).

Multiple instances open up the possibility to scale-out Benji for
performance reasons, to put instances where the backup source data is or to
have a dedicated instance for restores for example.

Locking between different instances is done via the central database.
