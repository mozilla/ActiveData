#!/usr/bin/env bash

sudo -i
export PYTHONPATH=.
export HOME=/home/ec2-user
cd ~/ActiveData
/usr/local/bin/gunicorn --pythonpath . --config resources/gunicorn/config.py 'active_data.app:main(settings="resources/config/staging_settings.json")'
