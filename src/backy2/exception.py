#!/usr/bin/env python
# -*- encoding: utf-8 -*-
class BackyException(Exception):
    pass

class UsageError(BackyException, RuntimeError):
    pass

class InvalidSourceError(BackyException, RuntimeError):
    pass

class AlreadyLocked(BackyException, RuntimeError):
    pass

class InternalError(BackyException, RuntimeError):
    pass

class ConfigurationError(BackyException, RuntimeError):
    pass

class NbdServerAbortedNegotiationError(BackyException, IOError):
    pass
