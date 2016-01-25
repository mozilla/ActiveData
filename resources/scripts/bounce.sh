curl -X PUT -d "{\"persistent\": {\"cluster.routing.allocation.enable\": \"none\"}}"  http://localhost:9200/_cluster/settings




curl -XPUT -d "{\"persistent\": {\"cluster.routing.allocation.enable\": \"all\"}}"  http://localhost:9200/_cluster/settings


curl -XPUT -d '{"index": {"number_of_replicas": 1}}'  http://localhost:9200/_settings


curl -XPUT -d '{"persistent" : {"cluster.routing.allocation.exclude._ip" : "172.31.0.39"}, "transient": {"cluster.routing.allocation.exclude._ip" : ""}}' http://localhost:9200/_cluster/settings
