#!/usr/bin/env bash


gunicorn --config resources/gunicorn.config.py active_data:app
