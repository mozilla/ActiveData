sudo supervisorctl stop es
sudo rm -fr /data1/active-data
sudo rm -fr /data2/active-data
sudo rm -fr /data3/active-data
sudo rm -fr /data4/active-data
sudo supervisorctl start es
tail -f /data1/logs/es.log
