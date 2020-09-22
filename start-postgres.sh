#!/usr/bin/env bash

set -Eeuo pipefail


export uri=postgresql:///tenmo?host=/run/postgresql
export PGDATA=/home/gleber/code/tenmo/db
if pg_ctl status; then
    pg_ctl stop
fi
if [[ "$*" == *purge* ]]; then
    rm -r $PGDATA
fi
if [[ ! -d $PGDATA ]]; then
    pg_ctl init
    pg_ctl start
    psql -d postgres -c 'create database tenmo;'
    psql $uri -f database.sql
else
    pg_ctl start
fi

echo export uri=$uri
