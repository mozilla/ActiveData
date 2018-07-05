


curl -XPUT -d "{\"transient\" : {\"indices.store.throttle.type\" : \"none\"}}" http://localhost:9200/_cluster/settings  -H "Content-Type: application/json"
curl -XPUT localhost:9200/unittest/_settings -d '{"index" : {"refresh_interval" : -1} }'





curl -XPUT -d "{\"transient\" : {\"indices.store.throttle.type\" : \"merge\"}}" http://localhost:9200/_cluster/settings


curl -XPUT localhost:9200/_cluster/settings -d "{\"persistent\" : { \"indices.recovery.max_bytes_per_sec\" : \"1000mb\"}}"  -H "Content-Type: application/json"



indices.store.throttle.max_bytes_per_sec" : "100mb"




curl -XPUT -d "{\"transient\" : {\"indices.store.throttle.type\" : \"merge\"}}" http://localhost:9200/_cluster/settings


curl -XPUT http://localhost:9200/_cluster/settings -d "{\"transient\":{\"indices.store.throttle.max_bytes_per_sec\":\"100mb\"}}" -H "Content-Type: application/json"



curl -XPUT http://localhost:9200/_cluster/settings -d "{\"transient\":{\"indices.store.throttle.type\":\"none\"}}" -H "Content-Type: application/json"


