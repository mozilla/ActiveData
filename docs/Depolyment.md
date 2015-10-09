

ETL
===

The ETL is covered by two projects

* [TestLog-ETL](https://github.com/klahnakoski/TestLog-ETL) (using the `etl` branch) - is the workhorse
* [SpotManager](https://github.com/klahnakoski/SpotManager) (using the `beta` branch) - responsible for deploying the above

  


Production Deployment Steps
---------------------------

In the event we are adding new ETL pipelines, we must make sure the schema is properly established before the army of robots start filling it.  Otherwise we run the risk of creating multiple table instances, all with similar names.

With new ETL comes new tables, new S3 buckets and new Amazon Queues.

1. Stop SpotManager
2. Terminate all ETL instances
3. Create S3 buckets and queues
4. Use development run ETL and confirm use of buckets and queues, and confirm creation of new tables in production
5. Push code to `etl` branch
6. Pull SpotManager changes (on `beta` branch) to its instance
7. Start Spot manager; 
8. Confirm nothing blows up; which will take several minutes as SpotManager negotiates pricing and waits to setup instances 
9. Run the backfill, if desired
10. Increase SpotManager budget, if desired
 

ActiveData Sevice
=================

The ActiveData service is a relatively simple, stateless, query translation service.  It was designed to be agnostic about schema changes and migrations.  Upgrading is simple.     

Production Deployment Steps
---------------------------

1. On the frontend machine run `git pull origin master`
2. Use supervisor to restart service

