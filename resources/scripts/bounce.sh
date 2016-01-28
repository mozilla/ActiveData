curl -X PUT -d "{\"persistent\": {\"cluster.routing.allocation.enable\": \"none\"}}"  http://localhost:9200/_cluster/settings




curl -XPUT -d "{\"persistent\": {\"cluster.routing.allocation.enable\": \"all\"}}"  http://localhost:9200/_cluster/settings


curl -XPUT -d '{"index": {"number_of_replicas": 1}}'  http://localhost:9200/_settings


curl -XPUT -d '{"persistent" : {"cluster.routing.allocation.exclude._ip" : "172.31.0.39"}, "transient": {"cluster.routing.allocation.exclude._ip" : ""}}' http://localhost:9200/_cluster/settings
curl -XPUT -d '{"persistent" : {"cluster.routing.allocation.exclude.zone" : "spot"}}' http://localhost:9200/_cluster/settings



curl -XPUT -d '{"persistent" : {"cluster.routing.allocation.exclude._ip" : ""}, "transient": {"cluster.routing.allocation.exclude._ip" : ""}}' http://localhost:9200/_cluster/settings
curl -XPUT -d '{"persistent" : {"cluster.routing.allocation.exclude.zone" : ""}}' http://localhost:9200/_cluster/settings


# KEEP THE MONSTER NODES OF THIS INDEX ON THE PRIMARY/SECONDARY FOR NOW
# MOVING TO SPOT TAKES SO LONG WE LOSE THE NODES BEFORE WE CAN USE THEM
curl -XPUT -d '{"index.routing.allocation.exclude.zone" : "spot"}' localhost:9200/unittest20150803_045709/_settings
