#!/usr/bin/env bash


cd ~/ActiveData
git pull origin better-balance
export PYTHONPATH=.

nohup python27 resources/scripts/es_fix_unassigned_shards.py --settings=resources/config/fix_unassigned_shards.json
tail -n200 -f ./results/fix_unassigned_shards.log
