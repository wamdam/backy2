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

Per default backy2 uses the config file at /etc/backy.cfg
Via command line switch '-c' or '--config-file' once can specify a different
config path. The following example uses a config file at
/etc/backy_my_test_vm.cfg for the backy2 commands:

backy2 -c my_test_vm.cfg initdb

Per default backy2 uses shared resources over all backups jobs, as resource
locking is done if multiple backup jobs are running. It is therefore important,
if multiple individual backup configs are used for concurrent running jobs,
that each job/config uses it's own resources:

- process_name
- lock_dir
- logfile
- engine
- DataBackend
- NBD cache-dir
