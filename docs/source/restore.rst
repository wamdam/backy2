.. include:: global.rst.inc

.. _restore:

Restore
=======

.. command-output:: benji restore --help

There are two possible restore options with Benji.

.. NOTE:: If you determined which *version* you want to restore it is a
    good idea to protect this *version* to prevent any accidental
    or automatic removal by any retention policy enforcement that you
    might have configure.

Full Restores
-------------

A full restore either saves the image into a file (i.e. an image file),
to a device (e.g. /dev/hda) or to a Ceph RBD volume.

The target is specified by the URI scheme. Examples::

    $ benji restore <version_uid> file:///var/lib/vms/myvm.img
    $ benji restore -f <version_uid> file:///dev/hdm
    $ benji restore <version_uid> rbd://pool/myvm_restore

If the target already exists, i.e.

- it is a device file
- the Ceph RBD volume exists
- the image file exists

you need to ``--force`` the restore. Without ``--force`` Benji will throw
an error and give you a hint::

    $ benji restore V1  file://RESTORE
        INFO: $ benji restore V1 file://RESTORE
       ERROR: Restore target RESTORE already exists. Force the restore if you want to overwrite it.

.. NOTE:: When restoring to a Ceph RBD volume, Benji will create this RBD
    volume for you if it does not exist.

If the restore target is full of zeros (or holes will read as zeros which is normally
the case), you can use the ``-s`` or ``--sparse`` option for faster restores.
With ``-s`` Benji will not write (i.e. skip) empty blocks or blocks that contain
only zeros. This will also normally lead to less space usage on the restore target.

Usually you can use ``-s`` if your restore target is

- a new/non-existing Ceph RBD volume
- a new/non-existing image file (Benji will then create a sparse file)

.. CAUTION:: If you use ``-s`` on existing images, devices or files, restoreed
    blocks which are sparse will not be written, so whatever random data was
    in there before the restore will remain.

NBD Server
----------

Benji comes with its own NBD server which when started exports all known *versions*.
These *versions* can then be mounted on any Linux host. The requirements on the
Linux host are:

- loaded ``nbd`` kernel module (``modprobe nbd`` as root)
- installed ``nbd-client`` program (RPM package ``nbd`` on RHEL/CentOS7/Fedora)

The ``nbd-client`` contacts Benji's NBD server and connects an exported *version* to
a NBD block device (``/dev/nbd*``) on the Linux host. After that your image data is
available as a block device. If the image contains a filesystem it can be mounted
normally. You can then search for the relevant files and restore them.

.. command-output:: benji nbd --help

Read-Only Mount
~~~~~~~~~~~~~~~

This command will run the NBD server in read-only mode and wait for incoming connections::

    $ benji nbd -r
        INFO: Starting to serve nbd on 127.0.0.1:10809

You can then create a new NBD block device (as root)::

    # modprobe nbd
    # nbd-client -N V0000000001 127.0.0.1 -p 10809 /dev/nbd0
    Negotiation: ..size = 10MB
    bs=1024, sz=10485760 bytes

    # partprobe might throw a few WARNINGs because we're read-only
    partprobe /dev/nbd0
    mount -o ro,norecovery /dev/nbd0p1 /mnt

If the image directly contains a filesystem you can skip the partprobe command
and mount the ``/dev/nbd0`` device directly. The ``norecovery`` mount option
is required as Benji  blocks all writes because of the ``-r`` option.

When the NBD server receives an incoming connection it will output something
like this::

     INFO: Incoming connection from 127.0.0.1:33714
    DEBUG: [127.0.0.1:33714]: opt=7, len=17, data=b'\x00\x00\x00\x0bV0000000001\x00\x00'
    DEBUG: [127.0.0.1:33714]: opt=1, len=11, data=b'V0000000001'
     INFO: [127.0.0.1:33714] Negotiated export: V0000000001
     INFO: nbd is read only.

When you're done::

    umount /mnt
    nbd-client -d /dev/nbd0

In the other console, benji will tell you that nbd has disconnected::

    INFO: [127.0.0.1:38316] disconnecting

You can then either reconnect or press ``ctrl+c`` to end Benji's NBD server.

You may also mount the volume from another server. Benji by default binds the
NBD server to 127.0.0.1 (i.e. localhost). For this server to be reachable from
the outside bind to 0.0.0.0::

    benji nbd -a 0.0.0.0 -r

You can also chose an appropriate port with the ``-p`` option.

Read-Write Mount
~~~~~~~~~~~~~~~~

In addition to providing read-only access, Benji also allows read-write access
in a safe way. This means, the backup **will not be modified**.
The command sequence is mostly the same (but note the missing ``-r``)::

    $ benji nbd
        INFO: Starting to serve nbd on 127.0.0.1:10809

Of course you can then mount the volume read/write and fix all potential
filesystem errors or database files or you can modify data in any way you want::

    # modprobe nbd
    # nbd-client -N V0000000001 127.0.0.1 -p 10809 /dev/nbd0
    Negotiation: ..size = 10MB
    bs=1024, sz=10485760 bytes

    # partprobe /dev/nbd0
    # mount /dev/nbd0p1 /mnt
    # echo 'test' > /mnt/tmp/test  # example!
    
Any writes to the device will initiate a copy-on-write (COW) of the 
original blocks to a new version that Benji will dynamically create for you.

After you disconnect from the nbd-client via::

    umount /mnt
    nbd-client -d /dev/nbd0

Benji will start to finalise the COW version. Depending on how many changes
have been done to the original image this will take some time!
::

    INFO: [127.0.0.1:46526] disconnecting
    INFO: Fixating version V0000000002 with 1024 blocks, please wait!
    INFO: Fixation done. Deleting temporary data, please wait!
    INFO: Finished.

You can then safely press ``ctrl+c`` in order to end the NBD server.

.. CAUTION:: If you ``ctrl+c`` before the last "INFO: Finished." is reported,
    your copy-on-write clone will not be written completely and thus be invalid.
    However, the original backup *version* **will be untouched in any case**.

You can see the created COW *version* with ``benji ls``::

    $ benji ls
        INFO: $ benji ls
    +---------------------+-------------+------+-----------------------------------------+----------+------------+-------+-----------+------+
    |         date        |     uid     | name | snapshot_name                           |     size | block_size | valid | protected | tags |
    +---------------------+-------------+------+-----------------------------------------+----------+------------+-------+-----------+------+
    | 2018-06-10T01:00:43 | V0000000001 | test |                                         | 41943040 |    4194304 |  True |   False   |      |
    | 2018-06-10T01:01:16 | V0000000002 | test | nbd-cow-V0000000001-2018-06-10T01:01:16 | 41943040 |    4194304 |  True |    True   |      |
    +---------------------+-------------+------+-----------------------------------------+----------+------------+-------+-----------+------+

The name will be the same as the original *version*. The snapshot_name will start
with the prefix *nbd-cow-* followed by the *version* UID followed by a timestamp.

The COW *version* will automatically be marked as protected by Benji to prevent
removal by any automatic retention policy enforcement that you might have 
configured. This makes sure that your repair work won't be destroyed. You will
have to unprotect the *version* manually when it is no longer needed. After
that you can remove it normally or wait for your retention policy enforcement 
to clean it up.

.. NOTE:: You can of course also restore the COW *version* like any other
    *version* with ``benji retore``.

.. NOTE:: You may safely delete the original *version* and the COW *version*
    independently of each other.


Restore of invalid Versions
---------------------------

During a restore Benji will compare each restored block's checksum to the
stored checksum in the metadata backend **no matter if the block has been
marked as invalid previously**. If it encounters a difference, the block
and all versions containing this block will be marked as invalid and a
warning will be given::

   ERROR: Checksum mismatch during restore for block 9 (UID 1-a) (is: e36cee7fd34ae637... should-be: dea186672147e1e3..., block.valid: True). Block restored is invalid.
    INFO: Marked block invalid (UID 1-a, Checksum dea186672147e1e3. Affected versions: V0000000001, V0000000002
    INFO: Marked version invalid (UID V0000000001)
    INFO: Marked version invalid (UID V0000000002)

Even when encountering such an error, Benji will continue to restore.

.. NOTE:: The philosophy behind this is that restores should always succeed, even if there
    is data corruption. Often invalid data is in irrelevant places or can be fixed later.
    You get as much of your data back as possible!