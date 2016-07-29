#!/usr/bin/env python
# -*- encoding: utf-8 -*-

class IO():

    def __init__(self, config, block_size, hash_function):
        pass


    def open(self, io_name, mode='r', force=False):
        """ Prepare and check anything needed by the ios,
        possibly open files and start threads. If mode is 'w', then
        the force parameter decides if the given target should be overwritten
        (True) or if a new target should be created (False).
        """
        raise NotImplementedError()


    def size(self):
        """ Return the size in bytes of the opened io_name
        """
        raise NotImplementedError()


    def read(self, block, sync=False):
        """ Add a read job for a Block """
        raise NotImplementedError()


    def get(self):
        """ Get the result of a read job, however this is not specific
        to which job. It just gets one. """
        raise NotImplementedError()


    def write(self, block, data):
        """ Writes data to the given block
        """
        raise NotImplementedError()


    def close(self):
        """ Close the io
        """
        raise NotImplementedError()
