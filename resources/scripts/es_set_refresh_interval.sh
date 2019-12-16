curl -XPUT localhost:9200/unittest/_settings -d '{"index" : {"refresh_interval" : "1h"} }'
curl -XPUT localhost:9200/coverage/_settings -d '{"index" : {"refresh_interval" : "1h"} }'  -H "Content-Type: application/json"

curl -XPUT localhost:9200/unittest/_settings -d '{"index" : {"refresh_interval" : "5m"} }'  -H "Content-Type: application/json"


curl -XPOST localhost:9200/unittest20180617_000000/_close
curl -XPUT  localhost:9200/unittest20180617_000000/_settings -d '{"index.translog.sync_interval": "60s"}'  -H "Content-Type: application/json"
curl -XPOST localhost:9200/unittest20180617_000000/_open



curl -XPOST localhost:9200/perf20180101_000000/_close
curl -XPUT  localhost:9200/perf20180101_000000/_settings -d '{"index.translog.sync_interval": "60s"}'  -H "Content-Type: application/json"
curl -XPOST localhost:9200/perf20180101_000000/_open



curl -XPOST localhost:9200/coverage20180617_000000/_close
curl -XPUT  localhost:9200/coverage20180617_000000/_settings -d '{"index.translog.sync_interval": "60s"}'  -H "Content-Type: application/json"
curl -XPOST localhost:9200/coverage20180617_000000/_open

curl -XPUT localhost:9200/task/_settings -d '{"index.refresh_interval": "60s"}'  -H "Content-Type: application/json"



curl -XPUT localhost:9200/_settings -d '{"index.refresh_interval": "60s"}'  -H "Content-Type: application/json"
curl -XPUT localhost:9200/_settings -d '{"index.translog.durability": "async"}'  -H "Content-Type: application/json"
curl -XPUT localhost:9200/_settings -d '{"index.translog.flush_threshold_size": "1000mb"}'  -H "Content-Type: application/json"
curl -XPUT localhost:9200/_settings -d '{"index.merge.scheduler.max_thread_count" : 1}' -H "Content-Type: application/json"


        "indices.store.throttle.max_bytes_per_sec" : "1000mb"

          "indices.store.throttle.type" : "none"


          "indices.store.throttle.type" : "merge"

          # to limit merge threads
          index.merge.scheduler.max_thread_count: 1

          thread_pool.bulk.size

curl -XPOST localhost:9200/_all/_close
curl -XPUT localhost:9200/_settings -d '{"index.translog.durability": "async"}'  -H "Content-Type: application/json"
curl -XPUT localhost:9200/_settings -d '{"index.translog.flush_threshold_size": "1000mb"}'  -H "Content-Type: application/json"
curl -XPUT localhost:9200/_settings -d '{"index.translog.sync_interval": "60s"}'  -H "Content-Type: application/json"
curl -XPOST localhost:9200/_all/_open




curl -XPUT localhost:9200/_settings -d '{"index.translog.durability": "request"}'  -H "Content-Type: application/json"

