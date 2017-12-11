curl -XPUT localhost:9200/unittest/_settings -d '{"index" : {"refresh_interval" : "1h"} }'





curl -XPUT localhost:9200/unittest/_settings -d '{"index" : {"refresh_interval" : "5m"} }'  -H "Content-Type: application/json"
