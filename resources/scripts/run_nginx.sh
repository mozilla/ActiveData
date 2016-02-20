#!/usr/bin/env bash


cd ~/ActiveData

git pull origin gunicorn2

sudo cp resources/config/nginx.conf /etc/nginx/nginx.conf


more /logs/nginx.pid


sudo /etc/init.d/nginx start


