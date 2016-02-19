#!/usr/bin/env bash


cd ~/ActiveData

git pull origin gunicorn2

sudo cp resources/config/nginx.conf /etc/nginx/nginx.conf


sudo /etc/init.d/nginx stop
sudo /etc/init.d/nginx start


sudo /etc/init.d/nginx -s reload
