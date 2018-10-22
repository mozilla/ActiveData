

curl -XPUT 'localhost:9200/task/_settings' -d '{"index" : {"max_inner_result_window": 100000}}'  -H "Content-Type: application/json"



