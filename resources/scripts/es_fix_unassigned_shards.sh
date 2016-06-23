#!/usr/bin/env bash
export PYTHONPATH=.

python27 resources/scripts/es_fix_unassigned_shards.py --settings=resources/config/fix_unassigned_shards.json
