#!/usr/bin/env bash


sudo -i
export PYTHONPATH=.
export HOME=/home/ec2-user
cd ~/ActiveData

/usr/bin/nginx -t -c resources/config/nginx.conf
