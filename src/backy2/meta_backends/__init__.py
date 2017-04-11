#!/usr/bin/env python
# -*- encoding: utf-8 -*-

class MetaBackend():
    """ Holds meta data """

    def __init__(self):
        pass

    def set_version(self, version_name, snapshot_name, size, size_bytes):
        """ Creates a new version with a given name and snapshot_name.
        size is the number of blocks this version will contain.
        Returns a uid for this version.
        """
        raise NotImplementedError()


    def set_stats(self, version_uid, version_name, version_size_bytes,
            version_size_blocks, bytes_read, blocks_read, bytes_written,
            blocks_written, bytes_found_dedup, blocks_found_dedup,
            bytes_sparse, blocks_sparse, duration_seconds):
        """ Stores statistics
        """
        raise NotImplementedError()


    def get_stats(self):
        """ Get statistics for all versions """
        raise NotImplementedError()


    def set_version_invalid(self, uid):
        """ Mark a version as invalid """
        raise NotImplementedError()


    def set_version_valid(self, uid):
        """ Mark a version as valid """
        raise NotImplementedError()


    def get_version(self, uid):
        """ Returns a version as a dict """
        raise NotImplementedError()


    def protect_version(self, uid):
        """ Protect a version """
        raise NotImplementedError()


    def unprotect_version(self, uid):
        """ Unprotect a version """
        raise NotImplementedError()


    def get_versions(self):
        """ Returns a list of all versions """
        raise NotImplementedError()


    def add_tag(self, version_uid, name):
        """ Adds a tag to a version versions """
        raise NotImplementedError()


    def remove_tag(self, version_uid, name):
        """ Removes a tag from a version versions """
        raise NotImplementedError()


    def set_block(self, id, version_uid, block_uid, checksum, size, _commit=True):
        """ Set a block to <id> for a version's uid (which must exist) and
        store it's uid (which points to the data BLOB).
        checksum is the block's checksum
        size is the block's size
        _commit is a hint if the transaction should be committed immediately.
        """
        raise NotImplementedError()


    def set_blocks_invalid(self, uid, checksum):
        """ Set blocks pointing to this block uid with the given checksum invalid.
        This happens, when a block is found invalid during read or scrub.
        """
        raise NotImplementedError()


    def get_block_by_checksum(self, checksum):
        """ Get a block by its checksum. This is useful for deduplication """
        raise NotImplementedError()


    def get_block(self, uid):
        """ Get a block by its uid """
        raise NotImplementedError()


    def get_blocks_by_version(self, version_uid):
        """ Returns an ordered (by id asc) list of blocks for a version uid """
        raise NotImplementedError()


    def rm_version(self, version_uid):
        """ Remove a version from the meta data store """
        raise NotImplementedError()


    def get_delete_candidates(self, dt=3600):
        raise NotImplementedError()


    def get_all_block_uids(self, prefix=None):
        """ Get all block uids existing in the meta data store """
        raise NotImplementedError()


    def export(self, f):
        raise NotImplementedError()


    def import_(self, f):
        raise NotImplementedError()


    def close(self):
        pass


