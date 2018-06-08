.. include:: global.rst.inc
.. _configuration:

Configuration
=============

Benji only needs to be configured once.

benji.yaml
----------

There is an example configuration which lists all possible configuration
options:

.. literalinclude:: ../../etc/benji.yaml

Configuration File Location
---------------------------

Benji will by default search the following locations for configuration files:

* /etc/benji.yaml
* /etc/benji/benji.yaml
* ~/.benji.yaml
* ~/benji.yaml

If multiple of these files exist, only the first file found is read.

In order to explicitly pass a configuration file, use the ``-c`` (or
``--configfile``) parameter.

Multiple Instance Installations
-------------------------------

You can run Benji multiple times on different machines or in different
containers simultaneously. The configurations will have to match!
this is the responsibility of the user and isn't checked by Benji.  Be
careful to shutdown all instances before making configuration changes that
could affect other instances (like adding an encryption key).

Multiple instances open up the possibility to scale-out Benji for
performance reasons, to put instances where the backup source data is or to
have a dedicated instance for restores for example.

Locking between different instances is done via the central database.
