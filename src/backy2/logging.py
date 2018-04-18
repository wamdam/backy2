#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import logging
import sys

logger = logging.getLogger(__name__)

def init_logging(logfile, console_level):  # pragma: no cover
    handlers = []

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter('%(levelname)8s: %(message)s'))
    console.setLevel(console_level)
    handlers.append(console)

    if not logfile is None:
        logfile = logging.FileHandler(logfile)
        logfile.setLevel(logging.INFO)
        logfile.setFormatter(logging.Formatter('%(asctime)s [%(process)d] %(message)s'))
        handlers.append(logfile)

    logging.basicConfig(handlers=handlers, level=logging.DEBUG)

    # make alembic quiet
    logging.getLogger('alembic').setLevel(logging.WARN)

    logger.info('$ ' + ' '.join(sys.argv))
