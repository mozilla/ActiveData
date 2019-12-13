



curl -XPOST http://localhost:9200/repo/_close

curl -XPUT http://localhost:9200/repo/_settings -d "{\"index.blocks.read_only_allow_delete\": false}" -H "Content-Type: application/json"
curl -XPUT http://localhost:9200/_settings -d "{\"index.blocks.read_only_allow_delete\": false}" -H "Content-Type: application/json"

    curl -XPUT -H "Content-Type: application/json" http://localhost:9200/_all/_settings -d '{"index.blocks.read_only_allow_delete": null}'
