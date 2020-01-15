

curl -XPOST http://localhost:9200/_cluster/reroute -d "{\"commands\":[{\"cancel\": {\"index\": \"coverage20190403_000000\",\"shard\": 6, \"node\":\"backup1\", \"allow_primary\":true}}]}"  -H "Content-Type: application/json"
curl -XPOST http://localhost:9200/_cluster/reroute -d "{\"commands\":[{\"cancel\": {\"index\": \"coverage20190403_000000\",\"shard\": 2, \"node\":\"backup1\", \"allow_primary\":true}}]}"  -H "Content-Type: application/json"




curl -XPUT http://localhost:9200/pulse20160225_224749/_settings -d"{\"index.recovery.initial_shards\": 1}"

curl -XPUT http://localhost:9200/_cluster/settings -d"{\"persistent\": {\"index.recovery.initial_shards\": 1}}"



curl -XPOST http://localhost:9200/_cluster/reroute -d"{\"commands\":[{\"cancel\": {\"index\": \"unittest20160729_131157\",\"shard\": 79, \"node\":\"primary\", \"allow_primary\":true}}]}"


curl -XPOST http://localhost:9200/_cluster/reroute -d"{\"commands\":[{\"cancel\": {\"index\": \"unittest20160810_175547\",\"shard\": 26, \"node\":\"secondary\", \"allow_primary\":true}}]}"
curl -XPOST http://localhost:9200/_cluster/reroute -d"{\"commands\":[{\"cancel\": {\"index\": \"unittest20160810_175547\",\"shard\": 31, \"node\":\"secondary\", \"allow_primary\":true}}]}"
curl -XPOST http://localhost:9200/_cluster/reroute -d"{\"commands\":[{\"cancel\": {\"index\": \"unittest20160810_175547\",\"shard\": 33, \"node\":\"secondary\", \"allow_primary\":true}}]}"
curl -XPOST http://localhost:9200/_cluster/reroute -d"{\"commands\":[{\"cancel\": {\"index\": \"unittest20160810_175547\",\"shard\": 39, \"node\":\"secondary\", \"allow_primary\":true}}]}"
curl -XPOST http://localhost:9200/_cluster/reroute -d"{\"commands\":[{\"cancel\": {\"index\": \"unittest20160810_175547\",\"shard\": 69, \"node\":\"secondary\", \"allow_primary\":true}}]}"
curl -XPOST http://localhost:9200/_cluster/reroute -d"{\"commands\":[{\"cancel\": {\"index\": \"unittest20160810_175547\",\"shard\": 77, \"node\":\"secondary\", \"allow_primary\":true}}]}"
curl -XPOST http://localhost:9200/_cluster/reroute -d"{\"commands\":[{\"cancel\": {\"index\": \"unittest20160810_175547\",\"shard\": 84, \"node\":\"secondary\", \"allow_primary\":true}}]}"



curl -XPOST http://localhost:9201/_cluster/reroute -d"{\"commands\":[{\"cancel\": {\"index\": \"coverage20200109_000000\",\"shard\": 17, \"node\":\"spot_54.213.171.5\", \"allow_primary\":false}}]}" -H "Content-Type: application/json"
curl -XPOST http://localhost:9201/_cluster/reroute -d"{\"commands\":[{\"cancel\": {\"index\": \"unittest20191215_000000\",\"shard\": 8, \"node\":\"spot_34.209.88.90\", \"allow_primary\":false}}]}" -H "Content-Type: application/json"

unittest20191215_000000
spot_52.37.164.231


curl -XPOST http://localhost:9200/_cluster/reroute -d"{\"commands\":[{\"move\": {\"index\": \".kibana\",\"shard\": 0,\"from_node\": \"secondary\",\"to_node\": \"spot_C4D2B23E\"}}]}"
curl -XPOST http://localhost:9200/_cluster/reroute -d"{\"commands\":[{\"move\": {\"index\": \"repo20160225_224705\",\"shard\": 2,\"from_node\": \"spot_00C8C755\",\"to_node\": \"spot_01B4ADC5\"}}]}"


curl -XPOST http://localhost:9200/_cluster/reroute -d"{\"commands\":[{\"move\": {\"index\": \"treeherder20160722_171527\",\"shard\": 12,\"from_node\": \"spot_00C8C755\",\"to_node\": \"spot_01B4ADC5\"}}]}"
curl -XPOST http://localhost:9200/_cluster/reroute -d"{\"commands\":[{\"move\": {\"index\": \"treeherder20160722_171527\",\"shard\": 13,\"from_node\": \"spot_00C8C755\",\"to_node\": \"spot_01B4ADC5\"}}]}"
curl -XPOST http://localhost:9200/_cluster/reroute -d"{\"commands\":[{\"move\": {\"index\": \"treeherder20160722_171527\",\"shard\": 14,\"from_node\": \"spot_00C8C755\",\"to_node\": \"spot_01B4ADC5\"}}]}"
curl -XPOST http://localhost:9200/_cluster/reroute -d"{\"commands\":[{\"move\": {\"index\": \"treeherder20160722_171527\",\"shard\": 15,\"from_node\": \"spot_00C8C755\",\"to_node\": \"spot_01B4ADC5\"}}]}"
curl -XPOST http://localhost:9200/_cluster/reroute -d"{\"commands\":[{\"move\": {\"index\": \"treeherder20160722_171527\",\"shard\": 17,\"from_node\": \"spot_00C8C755\",\"to_node\": \"spot_01B4ADC5\"}}]}"
curl -XPOST http://localhost:9200/_cluster/reroute -d"{\"commands\":[{\"move\": {\"index\": \"treeherder20160722_171527\",\"shard\": 25,\"from_node\": \"spot_00C8C755\",\"to_node\": \"spot_01B4ADC5\"}}]}"
curl -XPOST http://localhost:9200/_cluster/reroute -d"{\"commands\":[{\"move\": {\"index\": \"treeherder20160722_171527\",\"shard\": 26,\"from_node\": \"spot_00C8C755\",\"to_node\": \"spot_01B4ADC5\"}}]}"


curl -XPOST http://localhost:9200/_cluster/reroute -d"{\"commands\":[{\"move\": {\"index\": \"treeherder20160722_171527\",\"shard\": 1,\"from_node\": \"spot_00C8C755\",\"to_node\": \"spot_BAC8AE00\"}}]}"
curl -XPOST http://localhost:9200/_cluster/reroute -d"{\"commands\":[{\"move\": {\"index\": \"treeherder20160722_171527\",\"shard\": 2,\"from_node\": \"spot_BAC8AE00\",\"to_node\": \"spot_01B4ADC5\"}}]}"





curl -XPOST http://localhost:9200/_cluster/reroute -d"{\"commands\":[{\"allocate_replica\": {\"index\": \"unittest20180916_000000\",\"shard\": 59,\"node\": \"3\"}}]}"  -H "Content-Type: application/json"
