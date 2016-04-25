curl -XPUT localhost:9200/_cluster/settings -d '{"transient.cluster.routing.allocation.exclude._ip" : "10.0.0.1"}'
