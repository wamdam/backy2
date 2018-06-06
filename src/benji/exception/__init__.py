#!/usr/bin/env python
# -*- encoding: utf-8 -*-
class BenjiException(Exception):
    pass

class UsageError(BenjiException, RuntimeError):
    pass

class InputDataError(BenjiException, RuntimeError):
    pass

class AlreadyLocked(BenjiException, RuntimeError):
    pass

class InternalError(BenjiException, RuntimeError):
    pass

class ConfigurationError(BenjiException, RuntimeError):
    pass

class NoChange(BenjiException, RuntimeError):
    pass

class NbdServerAbortedNegotiationError(BenjiException, IOError):
    pass
