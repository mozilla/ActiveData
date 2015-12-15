curl -X PUT -d "{\"persistent\": {\"cluster.routing.allocation.enable\": \"none\"}}"  http://localhost:9200/_cluster/settings




curl -XPUT -d "{\"persistent\": {\"cluster.routing.allocation.enable\": \"all\"}}"  http://localhost:9200/_cluster/settings
