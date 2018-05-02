#!/usr/bin/env bash


# usage: gunicorn [OPTIONS] [APP_MODULE]
#
# where
#
# <APP_MODULE> = <MODULE_PATH> ":" <FLASK_OBJECT>
# <MODULE_PATH> = is a dot delimited path to the module
# <FLASK_OBJECT> = the actual flask object in module (do not called `run()`)

cd ~/ActiveData
export PYTHONPATH=.:vendor
export ACTIVEDATA_CONFIG=resources/config/staging_settings.json

/usr/local/bin/gunicorn --pythonpath .:vendor --config resources/config/gunicorn.py 'active_data.app:flask_app'
