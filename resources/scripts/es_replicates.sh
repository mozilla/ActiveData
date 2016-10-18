

curl -XPUT 'localhost:9200/saved_queries/_settings' -d '{"index" : {"number_of_replicas" : 1}}'
