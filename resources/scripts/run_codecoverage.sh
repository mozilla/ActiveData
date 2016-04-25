#!/usr/bin/env bash


export PYTHONPATH=.
cd ~/ActiveData
python ./active_data/jobs/codecoverage.py --settings=./resources/config/codecoverage.json
