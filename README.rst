What is backy2?
###############

backy2 is a deduplicating block based backup software.

The primary usecases for backy are:

* fast and bandwidth-efficient backup of ceph/rbd virtual machine images to S3
  or NFS storage
* backup of LVM volumes (e.g. from personal computers) to external USB disks


Main features
-------------

**Small backups**
    backy2 deduplicates while reading from the block device and only writes
    blocks once if they have the same checksum (sha512).

**Fast backups**
    With the help of ceph's ``rbd diff``, backy2 will only read the changed
    blocks since the last backup. We have virtual machines with 600GB backed
    up in about 30 seconds with <70MB/s bandwidth.

**Small required bandwidth to the backup target**
    As only changed blocks are written to the backup target, a small (i.e.
    gbit) connection is sufficient even for larger backups. Even with newly
    created block devices the traffic to the backup target is small, because
    these block devices usually are full of \\0 and are deduplicated before even
    reaching the target storage.

**As simple as cp, but as clever as backup needs to be**
    With a very small set of commands, good ``--help`` and intuitive usage,
    backy2 feels mostly like ``cp``. And that's intentional, because we think,
    a restore must be fool-proof and succeed even if you're woken up at 3am
    and are drunk.

    And it must be hard for you to do stupid things. For example, existing
    files or rbd volumes will not be overwritten unless you ``--force``,
    deletion of young backups will fail per default.

**Scrubbing with or without source data against bitrod and other data loss**
    Every backed up block keeps a checksum with it. When backy scrubs the backup,
    it reads the block from the backup target storage, calculates it's
    checksum and compares it to the stored checksum (and size). If the checksum
    differs, it's most likely that there was an error when storing or reading
    the block, or by bitrod on the backup target storage.

    Then, the block and the backups it belongs to, are marked 'invalid' and the
    block will be re-read for the next backup version even if ``rbd diff`` indicates
    that it hasn't been changed.

    Scrubbing can also take a percentage value for how many blocks of the backup
    it should scrub. So you can statistically scrub 16% each day and have a
    full scrub each week (16*7 > 100).

    .. NOTE:: Even invalid backups can be restored!

**Fast restores**
    With supporting block storage (like ceph/rbd), a sparse restore is
    possible. This means, sparse blocks (i.e. blocks which "don't exist" or are
    all \\0) will be skipped on restore.

**Parallel: backups while scrubbing while restoring**
    As backy2 is a long-running process, you will of course not want to wait
    until something has finished. So there are very few places in backy where
    a global lock will be applied (especially on cleanup which you can kill
    at any time to release the lock).

    So you can scrub, backup and restore (multiple times each) on the same
    machine.

**Does not flood your caches**
    When reading large pieces of data on linux, often buffers/caches get filled
    with this data (which in case of backups is essentially only needed once).
    backy2 instructs linux to immediately forget the data once it's processed.

**Backs up very large volumes RAM- and CPU efficiently**
    We backup multiple terabytes per vm (and this multiple times per night).
    backy2 typically runs in <1GB of RAM with these volume sizes. RAM usage
    depends mostly on simultaneous reads/writes which are configured through
    ``backy.cfg``.

**backups can be directly mounted**
    backy2 brings it's own nbd (network block device) server. So a simple linux
    command makes backups directly mountable - even on another machine::

        root@backy2:~# backy2 nbd --help
        usage: backy2 nbd [-h] [-a BIND_ADDRESS] [-p BIND_PORT] [-r] [version_uid]

        positional arguments:
          version_uid           Start an nbd server for this version

        optional arguments:
          -h, --help            show this help message and exit
          -a BIND_ADDRESS, --bind-address BIND_ADDRESS
                                Bind to this ip address (default: 127.0.0.1)
          -p BIND_PORT, --bind-port BIND_PORT
                                Bind to this port (default: 10809)
          -r, --read-only       Read only if set, otherwise a copy on write backup is
                                created.

        root@backy2:~# backy2 nbd 446781e2-2046-11e7-8594-00163e8c0370
            INFO: $ /usr/bin/backy2 nbd 446781e2-2046-11e7-8594-00163e8c0370
            INFO: Starting to serve nbd on 127.0.0.1:10809
            INFO: You may now start
            INFO:   nbd-client -l 127.0.0.1 -p 10809
            INFO: and then get the backup via
            INFO:   modprobe nbd
            INFO:   nbd-client -N <version> 127.0.0.1 -p 10809 /dev/nbd0

        root@backy2:~# partprobe /dev/nbd0
        root@backy2:~# mount /dev/nbd0p1 /mnt

    These mounts are read/write (unless you specify ``-r``) and writing to them
    creates a copy-on-write backup version (*i.e. the original version is not
    modified*).

**Automatic tagging of backup versions**
    You can tag backups with your own tags depending on your usecase. However,
    backy2 also tags automatically with these tags::

        b_daily
        b_weekly
        b_monthly

    It has a clever algorithm to detect how long the backup for any given image
    and this tag is ago and then tags again with the given tag. So you'll see
    a b_weekly every 7 days (if you keep these backups).

**Prevents you from doing something stupid**
    By providing a config-value for how old backups need to be in order to be
    able to delete them, you can't accidentially delete very young backups.

    Also, with ``backy protect`` you can protect versions from being deleted.
    This is very important when you need to restore a version which is suspect
    to be deleted within the next hours. During restore a lock will prevent
    deletion, however, by protecting it, it cannot be deleted until you decide
    that it's not needed anymore.

    Also, you'll need ``--force`` to overwrite existing files or volumes.

**Easy installation**
    Currently under ubuntu 16.04, you simply install the .deb. Please refer to
    :ref:`installation` for a detailed install process.

**Free and Open Source Software**
    Anyone can review the source code and audit security and functionality.
    backy2 is licensed under the LGPLv3 license (:ref:`license`).

