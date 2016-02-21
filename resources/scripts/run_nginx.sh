#!/usr/bin/env bash


cd ~/ActiveData

git pull origin master

sudo cp resources/config/nginx.conf /etc/nginx/nginx.conf

sudo /etc/init.d/nginx start

more /logs/nginx.pid

