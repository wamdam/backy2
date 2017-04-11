#!/usr/bin/env python
from migrate.versioning.shell import main

if __name__ == '__main__':
    main(url='sqlite:////home/dk/develop/backy2/tmp/backy.sqlite', repository='src/backy2/meta_backends/sql_migrations', debug='False')
