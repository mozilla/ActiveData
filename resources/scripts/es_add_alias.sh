

curl -XPOST http://54.148.242.195:9200/_aliases -d "{\"actions\":[{\"add\":{\"index\":\"coverage20180930_000000\",\"alias\":\"coverage\"}}]}"  -H "Content-Type: application/json"


curl -XPOST http://localhost:9200/_aliases -d "{\"actions\":[{\"add\":{\"index\":\"repo20181023_165516\",\"alias\":\"repo\"}}]}"  -H "Content-Type: application/json"
