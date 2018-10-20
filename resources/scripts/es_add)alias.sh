

curl -XPOST http://54.149.21.8:9200/_aliases -d "{\"actions\":[{\"add\":{\"index\":\"coverage20180930_000000\",\"alias\":\"coverage\"}}]}"  -H "Content-Type: application/json"
