# Caring for ActiveData

Updated June 2019


### Overview of Parts

Here are a couple of diagrams that will help with descriptions below

* [Logical View](https://github.com/mozilla/ActiveData/blob/dev/docs/ActiveData%20Architecture%20Logical%20181117.pdf)
* [Machine View](https://github.com/mozilla/ActiveData/blob/dev/docs/ActiveData%20Architecture%20181020.png)

### Reference Doc

Once you are familiar with this doc, then you will only need the reference doc: 

* [List of Moving Parts](https://docs.google.com/spreadsheets/d/1ReUDh94YBP8VgdRJe9KW0PJqJHkICQFvaSuyZQd0uJY/edit#gid=0)


## Setup

### Code

Tending to ActiveData will require

* **ActiveData code** - `git clone https://github.com/mozilla/ActiveData.git`
* **ActiveData-ETL code** - `git clone https://github.com/klahnakoski/ActiveData-ETL.git`
* **Elasticsearch Head** - `git clone https://github.com/mobz/elasticsearch-head.git`
* **esShardBalancer** - `git clone https://github.com/klahnakoski/esShardBalancer.git`

### Config

All the secrets are assumed to be in `~/private.json` on your development machine. All config files point to this file.

    {
        "fabric":{
            "key_filename": "path/to/private/key.pem",
        },
        "aws_credentials":{
            "aws_access_key_id":"id",
            "aws_secret_access_key" :"secret",
            "region":"us-west-2"
        }
    }

### Connections

All machines are listed in the moving parts, but these two are the most useful

* Frontend
* Manager

Use the AWS dashboard to ensure your IP is allowed to connect to each machine. 

#### Test Connection

**Linux**

Be sure to add the ssh private key: `ssh-add ~/.ssh/<key>.pem`, and test your connection:

    ssh ec2-user@activedata.allizom.org

**Windows**

Setup a Putty configuration (called "`active-data-frontend`").  Ensure you can connect.

#### Test Tunneling 

You must be able to tunnel from localhost:9201 to the frontend on port 9200 so you can access the ES cluster:

**Linux**

    ssh -v -N -L 9201:localhost:9200 ec2-user@activedata.allizom.org 

**Windows**

If you are using Windows, with Putty, then you can tunnelling to ES using SSH on windows

    plink -v -N -L 9201:localhost:9200 "active-data-frontend"

### AWS

There are three resource categories that may interest you in AWS:

* **EC2** - Listing all machines, and links to security groups. Security groups are used to open your local machine to ActiveData instances
* **SQS** - Used to monitor ETL backlog, and ingestion backlog
* **S3** - Used to peruse the raw data that went through the pipeline


## Daily Operations

### Machine Health

When logging into machines, always check the disk. Some logs do not roll over, and some crashes can fill the drives with crash reports
 
    df -h

### Check ES Health

The **coordinator** node is a small node on the **FrontEnd** that serves all query requests to the outside world, and acts as a circuit breaker for the rest of the cluster. It can be bounced without affecting ES the overall cluster. No queries can be serviced when this node is down. 

* Ensure your IP is allowed inbound to the `ActiveData-Frontend` group 
* Map port 9201 to remote port 9200:<br>
  Linux - `ssh -v -N -L 9201:localhost:9200 ec2-user@activedata.allizom.org`<br>
  Windows - `plink -v -N -L 9201:localhost:9200 "active-data-frontend"`
* Use Elasticsearch Head (connecting to `localhost:9201`) to verify ES is available, or not

If you can connect, then review the status:

> When ES suffers from node loss, the status endpoint will stall, making it appears ES is down. Attempt an ActiveData query `{"from":"unittest"}` to see if the query endpoint is still working.  If the query endpoint is still working, wait a bit. 

* **green** - everything ok.  IF there is a problem, then it must be elsewhere.
* **yellow** - If a large number of random shards are not allocated, then there was a recent loss of spot nodes, or an OutOfMemory event.
* **red** - look at which shards are not allocated: If it is just new indexes, then this state should disappear in a minute. If random shards are not allocated, then there has been a catastrophic failure over many machines proceed to [Emergency Proceedures](https://github.com/mozilla/ActiveData/blob/dev/docs/_Caring_for_ActiveData.md#Emergency+Procedures)

If you can not connect, then attempt to bounce the **coordinator** node. 

* Ensure your IP is allowed inbound to the `ActiveData-Frontend` group 
* Login to **FrontEnd** machine
* `sudo supervisorctl`
* `restart es`
* `tail -f es`

Watch the logs for connection problems. It will take a short while for the node to reconnect. If the node continues to complain it can not find the cluster, then proceed to [Emergency Proceedures](https://github.com/mozilla/ActiveData/blob/dev/docs/_Caring_for_ActiveData.md#Emergency+Procedures)

### Drive is full?

The problem may be caused by a full drive (`df -h`). This is most often caused by a crash dump filling the drive; remove it:

    sudo rm /usr/local/elasticsearch/*.hprof

If that is not the problem, then you can hunt down the big directories with:

    du -sh -- *


### Check ETL Pipeline Health

Use Elasticsearch Head, look at the coverage indexes: If the latest one is today, then everything is OK. If not, then look at the SQS queues to see the backlog. A backlog will happen if 

* The ES cluster has been recovering
* The push-to-es on the ES spot nodes is not running
* The ETL is not given enough money to cover the load
* The ETL is sending large amounts of errors with specifics 


### Check service throughput

If there is a problem, but ES is "fine", then cause might be `gunicorn` (the ActiveData service).

Check the nginx logs, to get a sense of the problem 

* Ensure your IP is allowed inbound to the `ActiveData-Frontend` group 
* Login to **FrontEnd** machine
* `cd /data1/logs`
* `tail -f nginx_access.log`

If the logs are busy, and returning errors, then maybe someone is making too many requests. If we know who it is, then we can ask them to stop.  We could try blocking IPs, but the one time this happened, it was from a distributed botnet, with too many ips.

Maybe bouncing the service will help:

* `sudo supervisorctl`
* `restart gunicorn`

If there are still problems, then "it is complicated": Coding will be required to filter out the problem. 


## Overview of Parts

### ActiveData Frontend

ActiveData has a web server on the **Frontend** machine.  It has a local instance of ES (with no data) to minimize search delays, and to improve search load balancing in the ES cluster.

**Nginx**

Nginx is used in production for three reasons.

- Serve static files (located at `~/ActiveData/active_data/public`)
- Serve SSH (The Flask server can deliver its own SSH, probably slower, if required)
- Distribute calls to other services; primarily the query service

A **copy** of the Configuration is `~/ActiveData/resources/config/nginx.conf`

**Gunicorn**

Gunicorn is used to pre-fork the ActiveData service, so it can serve multiple requests at once, and quicker than Flask. Since ActiveData has no concept of session, so coordinating instances is not required. 

Configuration is `~/ActiveData/resources/config/gunicorn.conf`

**ActiveData Python Program**

The ActiveData program is a stateless query translation service. It was designed to be agnostic about schema changes and migrations. Upgrading is simple.

**Production Deployment Steps**

Updating the web server is relatively easy

1. On the `frontend` machine run `git pull origin master`
2. `sudo supervisorctl restart gunicorn`

#### Config and Logs

* Configuration is `~/ActiveData/resources/config/supervisord.conf`
* Logs are `/data1/logs/active_data.log`


### ActiveData Manager

Overall, the **Manager** machine is responsible for running CRON jobs against ActiveData.  The code for these jobs are found on multiple repositories, under the `manager` branch.  

* The [Manager setup](https://github.com/klahnakoski/ActiveData-ETL/blob/manager/resources/scripts/setup_manager.sh) reveals the repositories being used 
* [CRON jobs](https://github.com/klahnakoski/ActiveData-ETL/blob/manager/resources/cron/manager.cron) is the list of actions being performed
* Logs are found at `/data1/logs`

#### Shard Balancer

the `esShardBalancer` is responsible for distributing shards over the cluster so that the shards are evenly distributed according to the capabilities of the machines available. 

> It is safe to turn off the `esShardBalancer` for hours at a time. When `esShardBalancer` is down, ES can recover lost shards, but it will be unable to balance those shards, or setup new ones.  Over ?days?, query speeds will get ever-slower as spot nodes are lost; more load goes to the three backup nodes, which will be unable to keep up, and will then crash, turning the cluster red. 

#### Spot Manager

The SpotManager is responsible for bidding on EC2 machines and setting them up. There are two instances running: One manages the ETL machines, and the other manages the ES cluster.  

> It is not safe to change the spending on the ES cluster; spending less will cause nodes to be shutdown. It is safe to mess with spending on the ETL pipeline, and it is safe to disable the cron jobs that execute the Spot Managers; but not for too many hours.

### TC Logger

This is a simple machine running a simple program the listens to the taskcluster pulse queues and writes the messages to S3. It has not been touched in years. It is listed in the moving parts.

> This is the only critical piece of the whole system: During failure, there is less than 1/2 hour before data starts getting lost.  

### ETL (Extract Transform Load)

The ETL is covered by two projects

* [ActiveData-ETL](https://github.com/klahnakoski/ActiveData-ETL) (using the `etl` branch) - is the main program
* [SpotManager](https://github.com/klahnakoski/SpotManager) (using the `manager` branch) - responsible for deploying instances of the above

#### Upgrading existing pipelines

Code pushed to the `etl` branch is deployed automatically by the SpotManager, but existing machines are not touched. Despite being spot instances, these ETL machines can run for days. To upgrade the existing running machines 

* Ensure your IP is allowed inbound to the `ActiveData-ETL` group 
* `export PYTHONPATH=.:vendor`
* `python activedata_etl\update_etl.py --settings=resources\settings\update_etl.json`

The program will login to 40 machines at a time; update the repo and bounce the ETL process on each.


#### Adding Pipelines

In the event we are adding new ETL pipelines, we must make sure the schema is properly established before the army of robots start filling it. Otherwise we run the risk of creating multiple table instances, all with similar names.

With new ETL comes new tables, new S3 buckets and new Amazon Queues.

1. Stop SpotManager
2. Terminate all ETL instances
3. Create S3 buckets and queues
4. Use development to run ETL and confirm use of buckets and queues, and confirm creation of new tables in production
5. Push code to `etl` branch
6. Pull SpotManager changes (on `manager` branch) to its instance
7. Start Spot manager;
8. Confirm nothing blows up; which will take several minutes as SpotManager negotiates pricing and waits to setup instances
9. Run the backfill, if desired
10. Increase SpotManager budget, if desired
 

### ElasticSearch

Elasticsearch is powerful magic. Only the ES developers really know the plethora of ways this magic will turn against you. There are only two paths forward: You have already performed the operation before on a multi-terabyte index, and you know the safe incantation. Or, you have not done this before, and you will screw things up. Feeling lucky? Please ensure you got the next two days free to fix your mistake: Everything is backed up to S3, so the worst case is you must rebuild the index. (Which has not been done for years, so the code is probably rotten).

#### Fixing Cluster

ES still breaks, sometimes. All problems encountered so far only require a bounce, but that bounce must be controlled. Be sure these commands are run on the **coordinator6** node (which is the ES master located on the **Frontend** machine).
 
 1. **Ensure all nodes are reliable** - This is a lengthy process, disable the SPOT nodes, or `curl -XPUT -d '{"persistent" : {"cluster.routing.allocation.exclude.zone" : "spot"}}' http://localhost:9200/_cluster/settings` If you do not do this step, a shutdown of a node may leave a single shard in the `spot` zone, which will be at risk of loss until it is replicated back to a safe node. 
 2. **Disable shard movement** - This will prevent the cluster from making copies of any shards that are on the bouncing node.  `curl -X PUT -d "{\"persistent\": {\"cluster.routing.allocation.enable\": \"none\"}}" http://localhost:9200/_cluster/settings`
 3. Bounce the nodes as you see fit
 4. **Enable shard movement** - `curl -XPUT -d "{\"persistent\": {\"cluster.routing.allocation.enable\": \"all\"}}" http://localhost:9200/_cluster/settings`
 5. **Re-enable spot nodes** - `curl -XPUT -d '{"persistent" : {"cluster.routing.allocation.exclude.zone" : ""}}' http://localhost:9200/_cluster/settings`

#### Changing Cluster Config

ES will take any opportunity to loose your data if you make any configuration changes. To prevent this you must ensure no important shards are on the node you are changing: Either it has no primaries, or all primaries are replicated to another node. You can ensure this happens by telling ES to exclude the machine you want cleared of shards. Due to enormous size of the ActiveData indexes, you will probably need to deploy more machines to handle the volume while you make a transition.  

    curl -XPUT -d '{"persistent" : {"cluster.routing.allocation.exclude._ip" : "172.31.0.39"}}' http://localhost:9200/_cluster/settings

be sure the transient settings for the same do not interfere with your plans: 

    curl -XPUT -d '{"transient": {"cluster.routing.allocation.exclude._ip" : ""}}' http://localhost:9200/_cluster/settings


 1. **Ensure all nodes are reliable** - This is a lengthy process, disable the SPOT nodes, or `curl -XPUT -d '{"persistent" : {"cluster.routing.allocation.exclude.zone" : "spot"}}' http://localhost:9200/_cluster/settings` If you do not do this step, a shutdown of a node may leave a single shard in the `spot` zone, which will be at risk of loss until it is replicated back to a safe node. 
 2. Move shards off the node you plan to change `curl -XPUT -d '{"persistent" : {"cluster.routing.allocation.exclude._ip" : "172.31.0.39"}}' http://localhost:9200/_cluster/settings`
 3. Wait while ES moves the shards of the node. *Jan2016 - took 1 day to move 2 terabytes off a node.* 
 4. Disable shard movement `curl -X PUT -d "{\"persistent\": {\"cluster.routing.allocation.enable\": \"none\"}}" http://localhost:9200/_cluster/settings`
 5. Stop node, perform changes, start node
 6. Enable shard movement `curl -XPUT -d "{\"persistent\": {\"cluster.routing.allocation.enable\": \"all\"}}" http://localhost:9200/_cluster/settings`
 7. Allow allocation back to node: `curl -XPUT -d '{"persistent" : {"cluster.routing.allocation.exclude._ip" : ""}}' http://localhost:9200/_cluster/settings`
 8. Re-enable spot nodes: `curl -XPUT -d '{"persistent" : {"cluster.routing.allocation.exclude.zone" : ""}}' http://localhost:9200/_cluster/settings`

Upon which another node can be upgraded


# Emergency Procedures

The ES cluster is red! Nodes are missing! Everything is broken!  


### Stop the esShardBalancer

The esShardBalancer will try it's best to bring ES to yellow state, even to the point of allowing data loss. ES will bring the cluster to yellow without loosing data, so it is best to let it do that.

* Ensure your IP is allowed inbound to the `ActiveData-Manager` group
* Login to **manager** machine
* Find PID of shardbalancer `tail -f  ~/esShardBalancer6/logs/balance.log`
* Kill it `kill -9 <pid>`

### Stop the FrontEnd

* Ensure your IP is allowed inbound to the `ActiveData-Frontend` group 
* Login to Frontend machine
* `sudo supervisorctl`
* `stop gunicorn`

### Stopping ETL

The ETL machines put a significant query load on the ES cluster. Stopping them will allow ES to heal faster, and reduce the number of errors emitted by the ETL machines.  

* Ensure your IP is allowed inbound to the `ActiveData-Manager` group
* Login to **manager** machine
* `nano SpotManager-ETL/examples/config/etl_settings.json`
* set price to zero 

> **Warning** - Be sure you are changing the "ETL" settings. The manager machine also has configuration for the ES cluster: dialing it down to zero will cause suffering

Here is an example, with price set to `2` (2 dollars per hour)

```javascript
{
    "budget": 2,  //MAXIMUM SPEND PER HOUR FOR ALL INSTANCES
    "max_utility_price": 0.02,   //MOST THAT WILL BE SPENT ON A SINGLE UTIL$
    "max_new_utility": 30,  //MOST NEW UTILITY THAT WILL BE REQUESTED IN A $
    "max_requests_per_type": 10,
    "max_percent_per_type": 0.70,  //ALL INSTANCE TYPES MAY NOT GO OVER THI$
    "price_file": "resources/aws/prices.json",
```

To resume ETL, turn the price back up. 

> Changing the price, either up or down, has no destructive effect; in the short term, you can set it as you please without loosing data. If set too low, a backlog will form, which will start to expire in 14 days, and then data is lost.


### Stopping ingestion

Every ES spot instance has an ingestion process which puts significant load on the ES cluster.

* Ensure your IP is allowed inbound to the `ActiveData es5` group
* Use your local machine
* `cd Activedata-ETL`
* `git checkout push-to-es6`
* `export PYTHONPATH=.:vendor`
* `python activedata_etl\update_push_to_es.py --settings=resources\settings\update_push_to_es.json --stop` 

What follows is an echo of the machine interactions as the program will connects to 10 machines at a time and tells supervisor to stop the `push-to-es` process. 

> Turning ingestion on, or off, has has no destructive effect in the short term. While off, a backlog is maintained for up to 14 days.


### Solve the problem

At this point the cluster is in a state where you can mess with it to bring it back to yellow.  If you are lucky, you just wait:  Use ES Head to monitor positive progress.

### Establish connection to a master node

Ensure you have a connection. Try to connect to any of the master nodes (the `coordinator` on the **Frontend** machine is also a master node). Try ssh login and connect locally (`curl http://localhost:9200/`) first, and bounce the node if it is down.

### Find ES OutOfMemory events 

Once you can connect to the cluster, you can use `find_es_oom.py` to review the logs; it will find any OoM events and bounce the problem nodes. 

* Ensure your IP is allowed inbound to the `ActiveData es5` group
* Use your local machine
* `cd Activedata-ETL`
* `git checkout push-to-es6`
* `export PYTHONPATH=.:vendor`
* `python activedata_etl\find_es_oom.py --settings=resources\settings\find_es_oom.json`

This will take a while, as only one node is reviewed at a time. Wait for the ES cluster to return to yellow

### Still RED?

A manual review of the nodes (starting with the backup nodes) will be required: Ensuring each ES instance can startup, and diagnosing what is going wrong.

### Run esShardBalancer locally

If the cluster continues to stay in the red state, then we must accept the cluster lost data.  If the `repo` or `saved_queries` indexes are missing primary shards, then the situation is really sad. This situation has not happened for years. The final resort is to run the esShardBalancer in `ACCEPT_DATA_LOSS=True` mode

* Ensure your IP is allowed inbound to the `ActiveData-Frontend` group 
* `cd ~/esShardBalancer`
* `git checkout es6`
* `python balance.py --settings=resources/config/dev_to_staging_es6/balance.json`


### Resume esShardBalancer on manager machine

When you get a yellow cluster, you can 

* Ensure your IP is allowed inbound to the `ActiveData-Manager` group
* Login to **manager** machine

```
    cd ~/esShardBalancer6
    export PYTHONPATH=.
    python27 balance.py --settings=resources/config/staging/balance.json >& /dev/null < /dev/null &
    disown -h
    tail -n200 -f logs/balance.log
```

### Resume the FrontEnd

The frontend will apply little load, you can start it back up as soon as ES is yellow

* Ensure your IP is allowed inbound to the `ActiveData-Frontend` group 
* Login to Frontend machine
* `sudo supervisorctl`
* `start gunicorn`

### Resume Ingestion

You should wait to resume ingestion until after the recent indexes are green. 

* Ensure your IP is allowed inbound to the `ActiveData es5` group
* Use your local machine
* `cd Activedata-ETL`
* `git checkout push-to-es6`
* `export PYTHONPATH=.;vendor`
* `python activedata_etl\update_push_to_es.py --settings=resources\settings\update_push_to_es.json --start` 

### Resume ETL

You should wait to resume ingestion until after the recent indexes are green. 
