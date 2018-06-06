.. include:: global.rst.inc

benji data layout
==================

benji uses two separate data storages: The *data backend* and the *meta
backend*.

The *data backend* stores the binary blocks whereas the *meta
backend* stores to which version the block belongs to, what it's checksum
is, wether it contains data (or it is sparse) and more.

For each backend there may be different implementations. Currently, there is
only one implementation for the *meta backend*, but two for the *data backend*.

meta backend
------------

The *meta backend* is responsible to manage all meta data for all backups.

.. ATTENTION:: As
    this is usually on a dedicated backup server which runs the benji process,
    it's recommended to somehow back this metadata up too. Without metadata
    no restore is possible.

    Please refer to the section :ref:`administration-guide-meta-storage` for HA-setups or the import/export
    feature of benji.

sql meta backend
~~~~~~~~~~~~~~~~

The *sql meta backend* relies on sqlalchemy, a python ORM which works with a
huge number of DBMS, e.g. MySQL, postgreSQL, sqlite, oracle.

For benji's purpose, you may use any of them depending a bit on how big your
backups are and how many versions you are storing. For a single workstation
backup with 10-20 versions, sqlite is perfectly suitable. However you will
benefit from postgreSQL's performance and stability when doing hundrets of
versions with terabytes of backup data.

To configure the *sql meta backend*, please refer to ``backy.cfg``'s section
``[MetaBackend]``::

    [MetaBackend]
    # Of which type is the Metadata Backend Engine?
    # Available types:
    #   benji.meta_backends.sql

    #######################################
    # benji.meta_backends.sql
    #######################################
    type: benji.meta_backends.sql

    # Which SQL Server?
    # Available servers:
    #   sqlite:////path/to/sqlitefile
    #   postgresql:///database
    #   postgresql://user:password@host:port/database
    engine: sqlite:////var/lib/benji/backy.sqlite

data backend
------------

There are currently two data backend implementations. Which one is in use is
determined by the ``backy.cfg`` configuration value in the section
``[DataBackend]`` called ``type``::

    [DataBackend]
    # Which data backend to use?
    # Available types:
    #   benji.data_backends.file
    #   benji.data_backends.s3


file data backend
~~~~~~~~~~~~~~~~~

The *file data backend* stores benji's blocks in 4MB files [1]_ in a
2-hierarchical directory structure::

    $ find /var/lib/benji/data
    /var/lib/benji/data
    /var/lib/benji/data/20
    /var/lib/benji/data/20/7d
    /var/lib/benji/data/20/7d/207d51da01kibRnRHsfsjdPkwGi9qLVU.blob
    /var/lib/benji/data/ea
    /var/lib/benji/data/ea/b2
    /var/lib/benji/data/ea/b2/eab2e98cee4yccDw2tf9j2HRkJUvDByG.blob
    …

There are several parameters in ``backy.cfg`` which can configure the *file data
backend*::

    [DataBackend]
    type: benji.data_backends.file

    # Store data to this path. A structure of 2 folders depth will be created
    # in this path (e.g. '0a/33'). Blocks of DEFAULTS.block_size will be stored
    # there. This is your backup storage!
    path: /var/lib/benji/data

    # How many writes to perform in parallel. This is useful if your backup space
    # can perform parallel writes faster than serial ones.
    simultaneous_writes: 5

    # How many reads to perform in parallel. This is useful if your backup space
    # can perform parallel reads faster than serial ones.
    simultaneous_reads: 5

    # Bandwidth throttling (set to 0 to disable, i.e. use full bandwidth)
    # bytes per second
    #bandwidth_read: 78643200
    #bandwidth_write: 78643200


s3 data backend
~~~~~~~~~~~~~~~

The *s3 data backend* stores benji's blocks in 4MB objects [1]_ in an S3
compatible storage (e.g. amazon s3, riak cs, ceph object gateway).

These are the parameters in ``backy.cfg`` to configure the *s3 data backend*::

    [DataBackend]
    type: benji.data_backends.s3

    # Your s3 access key
    aws_access_key_id: key

    # Your s3 secret access key
    aws_secret_access_key: secretkey

    # Your aws host (IP or name)
    host: 127.0.0.1

    # The port to connect to (usually 80 if not secure or 443 if secure)
    port: 10001

    # Use HTTPS?
    is_secure: false

    # Store to this bucket name:
    bucket_name: benji

    # How many s3 puts to perform in parallel
    simultaneous_writes: 5

    # How many reads to perform in parallel. This is useful if your backup space
    # can perform parallel reads faster than serial ones.
    simultaneous_reads: 5

    # Bandwidth throttling (set to 0 to disable, i.e. use full bandwidth)
    # bytes per second
    #bandwidth_read: 78643200
    #bandwidth_write: 78643200



.. [1] The size of the blobs can be configured in ``backy.cfg`` with the
    ``block_size: 4194304`` parameter. However, changing the ``block_size``
    on existing backup data will render all backups invalid.

