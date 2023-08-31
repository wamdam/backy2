#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import logging
import sys

logger = logging.getLogger(__name__)


class LevelFilter(logging.Filter):
    # Credits to Schore https://stackoverflow.com/users/4986349/schore
    # found at https://stackoverflow.com/questions/36337244/logging-how-to-set-a-maximum-log-level-for-a-handler
    def __init__(self, low, high):
        self._low = low
        self._high = high
        logging.Filter.__init__(self)
    def filter(self, record):
        if self._low <= record.levelno <= self._high:
            return True
        return False


def init_logging(logfile, console_level, debug=False):  # pragma: no cover
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter('%(levelname)8s: [%(name)s] %(message)s')),
    console.setLevel(console_level)
    console.addFilter(LevelFilter(console_level, logging.WARN))
    #logger.addHandler(console)

    error = logging.StreamHandler(sys.stderr)
    error.setFormatter(logging.Formatter('%(levelname)8s: [%(name)s] %(message)s')),
    error.setLevel(logging.ERROR)
    #logger.addHandler(console)

    logfile = logging.FileHandler(logfile)
    logfile.setLevel(logging.INFO)
    logfile.setFormatter(logging.Formatter('%(asctime)s [%(process)d] %(message)s')),
    #logger.addHandler(logfile)

    logging.basicConfig(handlers = [console, error, logfile], level=logging.DEBUG)

    logging.getLogger('alembic').setLevel(logging.WARN)
    logging.getLogger('boto3').setLevel(logging.WARN)
    logging.getLogger('botocore').setLevel(logging.WARN)
    logging.getLogger('urllib3').setLevel(logging.WARN)

    if debug:  # i.e. "-d" switch
        # debugging
        #loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
        console.setLevel(logging.DEBUG)
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger('backy2.logging').setLevel(logging.DEBUG)
        logging.getLogger('backy2').setLevel(logging.DEBUG)
        logging.getLogger('alembic').setLevel(logging.DEBUG)
        logging.getLogger('boto3').setLevel(logging.DEBUG)
        logging.getLogger('botocore').setLevel(logging.DEBUG)
        logging.getLogger('urllib3').setLevel(logging.DEBUG)
        log = logging.getLogger('requests.packages.urllib3')
        log.setLevel(logging.DEBUG)
        log.propagate = True

    logger.info('$ ' + ' '.join(sys.argv))


