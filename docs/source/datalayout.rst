.. include:: global.rst.inc

Data Layout
===========

Benji uses two separate data storages: The *data backend* and the *meta
backend*.

The *data backend* stores the binary blocks whereas the *meta
backend* stores to which version the block belongs, its checksum
but also whether it actually contains data or not.

The *meta backend* uses SQLAlchemy to access an underlying SQL database.
Benji has been tested with SQLite3 and PostgreSQL. For production deployments
PostgreSQL ist recommended.

The *data backend* is pluggable and there are currently three different
implementations from which you can choose:

- file: File based storage
- s3: S3 compatible storage like AWS S3, Google Storage, Ceph RADOS Gateway
  or Minio
- b2: Backblaze's B2 Cloud Storage

Meta Backend
------------

The *meta backend* is responsible for managing all metadata.

The *meta backend* relies on SQLAlchemy, a Python ORM which works with a
huge number of DBMS, e.g. MySQL, PostgreSQL, SQLite3 and Oracle.

Benji has been developed and test with PostgreSQL and SQLite3, so they are
the recommended database engines and you may encounter problems with other
databases. Patches to support other databases are welcome of course!

For Benji's purpose, you may use either PostgreSQL or SQLite3. But depending
on the amount of backup data and on how many versions you want to keep to
might want to chose one over the other. For a single workstation's backup
with 10-20 versions, SQLite3 is perfectly suitable. However you will
benefit from PostgreSQL's performance and stability when doing hundreds of
versions with terabytes of backup data.

You configure the location of your database with the ``metaBackend.engine``
directive. Please refer to the SQLAlchemy documentation for options and format
at http://docs.sqlalchemy.org/en/latest/core/engines.html.

Data Backend
------------

As mentioned above there are three data backend implementations. Please
refer to section :ref:`configuration`.

.. NOTE:: TODO: Information about the actual data layout
.. NOTE:: TODO: Encryption
.. NOTE:: TODO: Compression
.. NOTE:: TODO: Mention metadata accompanying blocks

