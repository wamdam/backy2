.. include:: global.rst.inc

backy2 configuration
====================

backy2 only needs to be configured once in order to define the *meta backend*
and *data backend*. Most other config options default to reasonable values.


.. _config_file:

backy.cfg
---------

This is the default backy.cfg:

.. literalinclude:: ../../etc/backy.cfg

Custom config file
------------------

backy2 will per default search the following locations for configuration files:

* /etc/backy.cfg
* /etc/backy/backy.cfg
* /etc/backy/conf.d/*
* ~/.backy.cfg
* ~/backy.cfg

In case multiple of these configurations exist, they are read in this order (later options
overwrite earier ones).

In order to explicitly pass a config file, use the ``-c`` (or ``--configfile``) parameter::

  backy2 -c ./my_test_vm.cfg ls

If you are using multiple config files, it's important for concurrency reasons
to have at least these parameters set differently in each configuration:

* ``process_name`` in section ``DEFAULTS``
* ``lock_dir`` in section ``DEFAULTS``
* ``logfile`` in section ``DEFAULTS``
* ``engine`` in section ``MetaBackend``
* ``path in`` section ``DataBackend``
* ``cachedir`` in section ``NBD``

If one of these are the same, no concurrency guarantees are given from backy2 and
in addition, cleanup jobs might then delete current backup data. You have been warned.

