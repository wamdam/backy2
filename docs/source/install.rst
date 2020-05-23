.. include:: global.rst.inc

Install
=======

Ubuntu Server 18.04
~~~~~~~~~~~~~~~~~~~

backy2
------

backy2 has a .deb file that you can download and install::

   wget <somewhere>/backy2_<version>.deb
   dpkg -i backy2_<version>.deb

This will complain about unfulfilled dependencies. Let's install them::

   apt install -f

As ubuntu 20.04 does not have the correct / latest versions
for backy2 to run (as we depend on the latest tech), let's install
them with the python package manager::

   apt install python3-pip
   pip3 install pycryptodome zstandard

Now, edit backy2 and replace the ``encryption_key`` by something
created by ::

   openssl rand -hex 32

…and set ``encryption_version`` to 1.


s3
--

If you want to use s3, you will need either the boto3 or minio
library. ``minio`` is faster in our tests but not compatible with
very old s3 versions::

   apt install python3-boto3
   pip3 install minio


postgresql
----------

As postgresql is the recommended dbms for backy2, let's install
and configure it::

   apt install postgresql python3-psycopg2
   sudo -u postgres psql

   $ psql (12.2 (Ubuntu 12.2-4))
   $ Type "help" for help.

   $ postgres=# create database backy2;
   $ CREATE DATABASE
   $ postgres=# create user root;
   $ CREATE ROLE
   $ postgres=# grant all privileges on database backy2 to root;
   $ GRANT
   $ postgres=# \q

Configure postgresql in backy2. For this edit ``/etc/backy.cfg`` and
change the meta backend in the section ``backy2.meta_backends.sql``
from the line ``engine: sqlite:////var/lib/backy2/backy.sqlite`` to::

  engine: postgresql:///backy2

.. NOTE::
   Of course you may also use a network-reachable postgresql server with
   username/password credentials and so on. Just use another connection
   string as provided as an example in the ``backy.cfg`` file.

sqlite
------

Nothing to be done here, however please only use sqlite for
testing.

mysql
-----

TODO

Initialization
--------------

Initialize the database::

  backy2 initdb
  backy2 ls

This should succeed.

