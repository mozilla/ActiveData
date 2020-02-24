

curl -XPUT 'localhost:9200/.monitoring-es-6-2020.02.20/_settings' -d '{"index" : {"number_of_replicas" : 3}}'  -H "Content-Type: application/json"



