curl -XPUT localhost:9200/unittest/_settings -d '{"index" : {"refresh_interval" : "1h"} }'
curl -XPUT localhost:9200/coverage/_settings -d '{"index" : {"refresh_interval" : "1h"} }'  -H "Content-Type: application/json"

curl -XPUT localhost:9200/unittest/_settings -d '{"index" : {"refresh_interval" : "5m"} }'  -H "Content-Type: application/json"


curl -XPUT localhost:9200/_settings -d '{"index.translog.durability": "async"}'  -H "Content-Type: application/json"
curl -XPUT localhost:9200/_settings -d '{"index.translog.flush_threshold_size": "1000mb"}'  -H "Content-Type: application/json"
curl -XPUT localhost:9200/_settings -d '{"index.translog.sync_interval": "60s"}'  -H "Content-Type: application/json"
