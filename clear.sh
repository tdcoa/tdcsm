#!/usr/bin/env bash

cd /opt/git/tdcsm

rm -r 0_override
rm -r 1_download
rm -r 2_sql_store
rm -r 3_ready_to_run
rm -r 4_output

rm coa.py
rm config.yaml
rm secrets.yaml
rm source_systems.yaml
rm motd.html
