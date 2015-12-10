curl -X PUT -d "{\"persistent\": {\"cluster.routing.allocation.enable\": false}}"  http://localhost:9200/_cluster/settings




curl -X PUT -d "{\"persistent\": {\"cluster.routing.allocation.enable\": true}}"  http://localhost:9200/_cluster/settings
