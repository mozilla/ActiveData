
curl -XPUT http://localhost:9200/_cluster/settings -d "{\"transient\": {\"cluster.routing.allocation.disk.watermark.low\": \"80%\"}}"
curl -XPUT http://localhost:9200/_cluster/settings -d "{\"persistent\": {\"cluster.routing.allocation.balance.primary\": 1}}"
curl -XPUT http://localhost:9200/_cluster/settings -d "{\"persistent\": {\"cluster.routing.allocation.enable\": \"all\"}}"
