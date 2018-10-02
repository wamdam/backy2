.. include:: global.rst.inc
.. meta::
    :description: Benji Backup Documentation:  A block based deduplicating backup software for Ceph RBD, image files and devices
    :keywords: backup,deduplication,ceph,lvm,block-based,s3,b2,rook,kubernetes

Benji, backup me!
=================

.. raw:: html

    <asciinema-player src="_static/quickstart.cast" 
        cols="160" rows="20" 
        autoplay="yes" preload="yes"
        loop="yes" speed="1.2" idle-time-limit="0.5">
    </asciinema-player>

Source Code
-----------

The source code is hosted on GitHub at `https://github.com/elemental-lf/benji <https://github.com/elemental-lf/benji>`_.
See section :ref:`installation` for installation instructions.

.. include:: ../../README.rst

.. toctree::
    :maxdepth: 2

    installation
    quickstart
    configuration
    backup
    scrub
    restore
    enforce
    cleanup
    administration
    datalayout
    container
    backy2
    support
    licenses
