TODOs
=====

Probably soonish
----------------

* Enhance metadata backup on data backend feature

  * Implement bulk import of version metadata
  * List version metadata

* Move from tags to labels

   * Filter version listings by label selector
   * Do scrubbing and expiry in relation to a label selector

* Finish key rotation support
* Perform fsfreeze as part of script framework
  (see https://gitlab.com/costrouc/kubernetes-rook-backup/blob/master/rook-backup.py#L115)
* Work on database migration support (Alembic)
* Move Rook image and scripts to a more generic but still Kubernetes centered solution

Unsorted collection
-------------------

* Reintroduce Debian or RPM packaging or PEX
* Write a new Makefile for build, test and release
* Write more tests
* Readd documentation for development setup
* Add tests for anything where scrub marks blocks as invalid (source changed,
  bit rot in backup, ...
* Add tests for CLI frontend
* Convert tests (back) to py.test or nose2?
* Check if we really should do image.close() ioctx.close() cluster.shutdown() as
  recommended in http://docs.ceph.com/docs/jewel/rbd/librbdpy/
* io._reader, io.get could return checksum in block (block._replace)...
* Support for multiple data backends
* Support for layering data backends to implement things like mirroring
* Native Google Storage backend
* Better NBD server performance (if possible)
* Deduplication and sparse detection in NBD fixate()?
* Make some more ASCIInema casts for the documentation
* Update website (the thing in the website subdirectory)
* Add script to generate hints from LVM usage bitmaps for classic and thin snapshots
* Reimplement partial full cleanup
