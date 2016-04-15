#!/usr/bin/env python
# -*- encoding: utf-8 -*-

class Reader():

    def __init__(self, config, block_size, hash_function):
        pass


    def open(self, source):
        """ Prepare and check anything needed by the readers,
        possibly open files and start threads """
        raise NotImplementedError()


    def size(self):
        """ Return the size in bytes of the opened source
        """
        raise NotImplementedError()


    def read(self, block, sync=False):
        """ Add a read job for a Block """
        raise NotImplementedError()


    def get(self):
        """ Get the result of a read job, however this is not specific
        to which job. It just gets one. """
        raise NotImplementedError()


    def close(self):
        """ Close the source
        """
        raise NotImplementedError()
