#!/usr/bin/env bash

sudo -i
export PYTHONPATH=.
export HOME=/home/ec2-user
cd ~/ActiveData


# usage: gunicorn [OPTIONS] [APP_MODULE]
#
# [APP_MODULE] IS A POORLY DOCUMENTED MINI-LANGUAGE:
#
# <APP_MODULE> = <MODULE_PATH> ":" <METHOD_NAME> <OPTIONAL_PARAMETERS>
#
# where
#
# <MODULE_PATH> = is a dot delimited path to the module
# <METHOD_NAME> = name of method in the module
# <OPTIONAL_PARAMETERS> = when concatenated with <METHOD_NAME> results in the ?Python? syntax method call.

/usr/local/bin/gunicorn --pythonpath . --config resources/gunicorn/config.py 'active_data.app:main(settings="resources/config/staging_settings.json")'
