Call this to see the full demo::

    $ ./init.sh
    $ backy2 ls
    $ ./due.sh
    $ ./sla.sh
    $ ./backup.sh s1
    $ ./due.sh

    # wait 1 minute

    $ ./due.sh

    # wait another minute until sla says backup too old

    $ ./sla.sh
    $ ./backup.sh s1

    # Also try
    $ ./cleanup.sh

    # when you're done, reset the playground:
    $ ./reset.sh
