#!/usr/bin/env bash


cd ~/ActiveData

git pull origin master


sudo cp resources/config/nginx.conf /etc/nginx/nginx.conf


sudo /etc/init.d/nginx start

more /data1/logs/nginx.pid




# RESTART

sudo kill -SIGINT `cat /data1/logs/nginx.pid`
sleep 2
sudo /etc/init.d/nginx start
