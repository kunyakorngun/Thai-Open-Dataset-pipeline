#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username "postgres" <<-EOSQL
    CREATE DATABASE warehouse;
    CREATE DATABASE mart;
EOSQL
