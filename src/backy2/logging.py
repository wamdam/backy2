#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import logging
import sys

logger = logging.getLogger(__name__)

def init_logging(logfile, console_level, debug=False):  # pragma: no cover
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter('%(levelname)8s: [%(name)s] %(message)s')),
    console.setLevel(console_level)
    #logger.addHandler(console)

    logfile = logging.FileHandler(logfile)
    logfile.setLevel(logging.INFO)
    logfile.setFormatter(logging.Formatter('%(asctime)s [%(process)d] %(message)s')),
    #logger.addHandler(logfile)

    logging.basicConfig(handlers = [console, logfile], level=logging.DEBUG)

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


