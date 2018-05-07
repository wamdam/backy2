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

    # silence alembic
    logging.getLogger('alembic').setLevel(logging.WARN)
    # silence filelock
    logging.getLogger('filelock').setLevel(logging.WARN)

    logger.info('$ ' + ' '.join(sys.argv))

# Source: https://stackoverflow.com/questions/6234405/logging-uncaught-exceptions-in-python/16993115#16993115
def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

sys.excepthook = handle_exception
