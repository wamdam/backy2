#!/usr/bin/env python
# -*- encoding: utf-8 -*-
class BackyException(Exception):
    pass

class UsageError(BackyException, RuntimeError):
    pass

class InputDataError(BackyException, RuntimeError):
    pass

class AlreadyLocked(BackyException, RuntimeError):
    pass

class InternalError(BackyException, RuntimeError):
    pass

class ConfigurationError(BackyException, RuntimeError):
    pass

class NoChange(BackyException, RuntimeError):
    pass

class NbdServerAbortedNegotiationError(BackyException, IOError):
    pass
