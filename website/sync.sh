#!/bin/bash
rsync --delete --exclude docs -avzP -e 'ssh -p 11322' src/* user@s2.d9tready.com:/var/www
cd ../docs
. ../env/bin/activate
make html
rsync --delete -avzP -e 'ssh -p 11322' build/html/* user@s2.d9tready.com:/var/www/docs
