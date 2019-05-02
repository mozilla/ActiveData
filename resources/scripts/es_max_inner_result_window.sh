

curl -XPUT http://localhost:9201/debug_active_data/_settings -d "{\"index\" : {\"max_inner_result_window\": 100000}}" -H "Content-Type: application/json"
curl -XPUT http://localhost:9201/debug_active_data/_settings -d "{\"index\" : {\"max_result_window\": 100000}}" -H "Content-Type: application/json"





