.. include:: global.rst.inc

Develop
=======

In order to develop backy2, follow these steps. This is all tested in ubuntu
16.10 and 16.04 *as user*. If anything in backy2 requires root, it will be
explicitly mentioned (hint: it's not much!).

Checkout the repository::

    $ git clone https://github.com/wamdam/backy2
    $ cd backy2

Create a virtualenv::

    $ make env

.. TODO: Prerequisits? psycop headers, mysql headers, sqlite, ???

This will install all dependent python packages as well as all requirements
for tests and documentation building.

To see what this does, just look at the Makefile. It's not magic.


Getting version information
---------------------------

In order to get version information, run::

    make info

Reset
-----

To rebuild anything, at any time simply run ::

    make clean

As this will also remove your virtualenv, re-run ::

    make env

after this.


Running tests
-------------

There are very few pytest test cases. We hope to have more in the future.
Anyway, you can run them via ::

    make test

.. NOTE:: pytest tests are currently in a disasterous state. Don't run them,
    they'll fail.

The much more challenging test cases are in a scripted smoketest. This test
runs 100 backups (including hint files, even with different image sizes),
scrubs and restores and compares all these. This is and will be the realistic
implementation test for backup, scrub and restore. **This must not fail**::

    make smoketest

Running backy2
--------------

In order to run backy2, source the virtualenv::

    $ . env/bin/activate
    $ backy2 --help

After that, you may follow the :ref:`backup` section in the quickstart tutorial.

Creating a debian package
-------------------------

Follow these steps to create a new .deb release:

1. Update the changelog in ``debian/changelog``. Just follow the existing
   formatting. Please ignore false weekday names - they're from me as I'm very
   lazy with this.

2. Change the version in ``setup.py``

3. ``make deb``

The new .deb file will be stored in ``../backy2_<version>_all.deb``.

Creating a .pex package
-----------------------

A .pex file is a zip file containing an entry point and all python libraries
including all compiled shared libraries required to run the software.

Python directly supports zip files, so it's able to directly run a .pex file.
You need to have a very similar build environment for your .pex file as it is
on your server in order to make this work.

But if you have this, the ``build/backy2.pex`` file will be a single file
deployment for your servers.

This is how you create it::

    make build/backy2.pex

In order to run this, simply call ::

    deactivate  # deactivate the virtualenv
    ./build/backy2.pex --help

Building the docs
-----------------

To build the docs, run::

    . env/bin/activate
    cd docs
    make html


The active virtualenv is needed because backy2 docs make heavy use of the
``.. command-output::`` plugin which is used to render backy2's output into the
docs.

The built html docs are then in ``build/html/index.html``.

Hints
-----

- Data backends are in ``src/backy2/data_backends``. Their abstract implemetation
  is in ``src/backy2/data_backends/__init__.py``.

  Which data backend is in use is directly defined in ``backy.cfg`` in the
  section ``[DataBackend]`` under the key ``type``. Example:
  ``type: backy2.data_backends.file``.
- Meta backends are in ``src/backy2/meta_backends``. Their abstract implemetation
  is in ``src/backy2/meta_backends/__init__.py``.

  Which meta backend is in unse is directly defined in ``backy.cfg`` in the
  section ``[MetaBackend]`` under the key ``type``. Example:
  ``type: backy2.meta_backends.sql``.
- The SQL migrations are in ``src/backy2/meta_backends/sql_migrations``.
  They are automatically generated with the script ``alembic``.
  This script only shortcuts the ``-c`` option. The call is then
  ``./alembic revision --autogenerate -m "Added snapshot_name to versions"``.
- The NBD server is in the directory ``src/backy2/enterprise``.

I don't need to tell you about pull requests, do I?

Have fun ;)


