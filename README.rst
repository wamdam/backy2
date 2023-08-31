What's the state of this project?
#################################

backy2 is an artefact of my hosting company where I recognized that there are
no sane and reliable backup solutions out there for block-based / snapshot-based
backup in the many-terabyte to petabyte range.

backy2 was the second backup software I wrote for our usecases. The first one
(you guessed rightm it was called "backy") was designed for .img based virtual
machines and had designated features for that.

backy2 was designed around our mostly ceph/rbd-based cluster, that's where the
most features come from.

Meanwhile we have switched to local lvm thin pools and we have a very clever,
fast and new pull-backup solution, for which I (of course) wrote the third
iteration of backup software, however it's neither called backy3, nor it's open
source (for now).

That's why this project receives only very minimal maintenance from me and practically
no updates. This means you can expect to have installation issues due to
non-existing libraries on modern operating system versions. However they should
be rather easy to be fixed and in some regard I'm willing to do this. So there's
no reason to panic if you're using backy2 in 2022 and plan to use it for the
next years. The python code will most likely be valid in several years and libs
have shown to be very compatible in never versions in the last years.

The code however is stable (no guarantees though), the software is feature
complete and that's the main reason why there have been no significant commits
in the last months. We have performend many years of stable backups and restores
with it.


What is backy2?
###############

backy2 is a deduplicating block based backup software which encrypts and
compresses by default.

The primary usecases for backy are:

* fast and bandwidth-efficient backup of ceph/rbd virtual machine images to S3
  or NFS storage
* backup of LVM volumes (e.g. from personal computers) to external USB disks


Main features
-------------

**Small backups**
    backy2 deduplicates while reading from the block device and only writes
    blocks once if they have the same checksum (sha512).

**Compressed backups**
   backy2 compresses all data blocks with respect to performance with the
   zstandard library.

**Encrypted backps**
   All data blocks are encrypted by default. Encryption is managed in integer
   versions, migration and re-keying procedures exist.

**Fast backups**
    With the help of ceph's ``rbd diff``, backy2 will only read the changed
    blocks since the last backup. We have virtual machines with 600GB backed
    up in about 30 seconds with <70MB/s bandwidth.

**Continuable backups and restores**
    If the data backend storage is unreliable (as in storage, network, …)
    and backups or restores can't finish, backy2 can continue them when the
    outage has ended.

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
    a global lock will be applied (especially on a very rarely used full
    cleanup which you can kill at any time to release the lock).

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
    We have seen ~16GB of RAM usage with large configured queues for 200TB
    images and a backup performance of Ø350MB/s to an external s3 storage.

**backups can be directly mounted**
    backy2 brings it's own fuse service. So a simple linux command makes
    backups directly mountable - even on another machine::

        root@backy2:~# backy2 fuse /mnt

   And on another terminal::

        root@backy2:~# ls -la /mnt/by_version
        drwx------ 0 root root 0 Mai  3 16:14 0c44841a-8d47-11ea-8b2d-3dc6919c2aca
        drwx------ 0 root root 0 Mai  3 16:14 60ae794e-8d46-11ea-8b2d-3dc6919c2aca
        drwx------ 0 root root 0 Mai  3 16:14 9d8cfe80-8d46-11ea-8b2d-3dc6919c2aca

        root@backy2:~# ls -la /mnt/by_version_uid/9d8cfe80-8d46-11ea-8b2d-3dc6919c2aca
        -rw------- 1 root root 280M Mai  3 14:01 data
        -rw------- 1 root root    0 Mai  3 14:01 expire
        -rw------- 1 root root    9 Mai  3 14:01 name
        -rw------- 1 root root    0 Mai  3 14:01 snapshot_name
        -rw------- 1 root root   51 Mai  3 14:01 tags
        -rw------- 1 root root    5 Mai  3 14:01 valid

        root@backy2:~# cat /mnt/by_version_uid/9d8cfe80-8d46-11ea-8b2d-3dc6919c2aca/name
        sometest1

        root@backy2:~# mount /mnt/by_version_uid/9d8cfe80-8d46-11ea-8b2d-3dc6919c2aca/data /mnt

    You get the idea. The data file (and resulting partitions, mounts) read/write!
    Writing to them will write to a temporary local file. The original backup version
    is *not* modified!
    This means, you may even boot a VM from this file from a remote backup.

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
    Currently under ubuntu 18.04, you simply install the .deb. Please refer to
    :ref:`installation` for a detailed install process.

**Free and Open Source Software**
    Anyone can review the source code and audit security and functionality.
    backy2 is licensed under the LGPLv3 license (:ref:`license`).

