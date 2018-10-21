#!/usr/bin/env bash


cd ~/ActiveData

git pull origin master

# sudo kill -SIGINT `cat /data1/logs/nginx.pid`

sudo cp resources/config/nginx.conf /etc/nginx/nginx.conf


sudo /etc/init.d/nginx start

more /data1/logs/nginx.pid

