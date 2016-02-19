#!/usr/bin/env bash

sudo -i
export PYTHONPATH=.
export HOME=/home/ec2-user
cd ~/ActiveData
/usr/local/bin/gunicorn --config resources/gunicorn/config.py active_data:app:main(settings=
