curl -XPUT localhost:9200/unittest/_settings -d '{"index" : {"refresh_interval" : "1h"} }'
curl -XPUT localhost:9200/coverage/_settings -d '{"index" : {"refresh_interval" : "1h"} }'  -H "Content-Type: application/json"

curl -XPUT localhost:9200/unittest/_settings -d '{"index" : {"refresh_interval" : "5m"} }'  -H "Content-Type: application/json"
