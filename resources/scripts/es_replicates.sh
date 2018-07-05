

curl -XPUT 'localhost:9200/coverage20180617_000000/_settings' -d '{"index" : {"number_of_replicas" : 0}}'  -H "Content-Type: application/json"
