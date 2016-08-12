#!/usr/bin/env python
# -*- encoding: utf-8 -*-

class DataBackend():
    """ Holds BLOBs, never overwrites
    """

    # Does this filestore support partial reads of blocks?
    #
    # # Does this filestore support partial reads of blocks?
    _SUPPORTS_PARTIAL_READS = False
    _SUPPORTS_PARTIAL_WRITES = False

    def __init__(self, path):
        self.path = path


    def save(self, data):
        """ Saves data, returns unique ID """
        raise NotImplementedError()


    def update(self, uid, data, offset=0):
        """ Updates data, returns written bytes.
        This is only available on *some* data backends.
        """
        raise NotImplementedError()


    def read(self, uid, offset=0, length=None):
        """ Returns b'<data>' or raises FileNotFoundError.
        With length==None, all known data is read for this uid.
        """
        raise NotImplementedError()


    def rm(self, uid):
        """ Deletes a block """
        raise NotImplementedError()


    def rm_many(self, uids):
        """ Deletes many uids from the data backend and returns a list
        of uids that couldn't be deleted.
        """
        raise NotImplementedError()


    def get_all_blob_uids(self, prefix=None):
        """ Get all existing blob uids """
        raise NotImplementedError()


    def close(self):
        pass


