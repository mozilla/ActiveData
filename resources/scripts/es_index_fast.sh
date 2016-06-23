


curl -XPUT -d "{\"transient\" : {\"indices.store.throttle.type\" : \"none\"}}" http://localhost:9200/_cluster/settings
curl -XPUT localhost:9200/unittest/_settings -d '{"index" : {"refresh_interval" : -1} }'





curl -XPUT -d "{\"transient\" : {\"indices.store.throttle.type\" : \"merge\"}}" http://localhost:9200/_cluster/settings
