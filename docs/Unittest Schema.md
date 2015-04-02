
Unittest Logs 
=============

The Unittest logs are deep JSON structures describing each of the individual test results.  There are five major properties

`result` Columns
----------------

Describes properties exclusive to an individual test result

* `result.test` - *string* path and filename of the specific test
* `result.result` - *string* recording the test result
* `result.expected` - *string* representing what was expected of the test.  Not all tests are expected to pass.
* `result.ok` - *boolean* is true if result matches expected result 
* `result.start_time` -  *timestamp* when the test started running  
* `result.end_time` - *timestamp* when the test ended
* `result.duration` - *seconds* usually the calculated difference between start and end times, but could be the test's own reported duration
* `result.last_log_time` - *timestamp* of the last log line seen for this run, may be after the `result.end_time`
* `result.stats` - *object* counts the sub-tests grouping some other features of the test not found in the structured log
* `result.stats.fail` - *count*
* `result.stats.notrun` - *count*
* `result.stats.timeout` - *count*
* `result.stats.pass` - *count*
* `result.missing_test_end` - *boolean* missing the test's end record
* `result.missing_test_start` - *boolean* missing the test's start record
* `result.crash` - *boolean* the structured log recorded `"action": "crash"`

`run` Columns
-------------

Properties that describe the run of this test suite, and the many tests that are contained within.  Most are verbatim from the Pulse messages received by the ETL, and replicated for every test result in the data store.  **If you are interested in suite-level aggregates, be sure to filter by the suite's canonical test result: `"where": {"eq":{"etl.id":0}}`**

* `run.suite` - *string* name of the suite
* `run.chunk` - *integer* each suite is broken into chucks to parallelize the run, each chunk is given a number
* `run.insertion_time` - *timestamp* ?time submitted to job queue for execution?
* `run.timestamp` - *timestamp*
* `run.job_number` - *string* 
* `run.files` - *array* of files recorded for this suite, one or more of which are the structured log digested to make this record 
* `run.files.name` - *string* name of the file
* `run.files.url` - *string* url where he contents can/could be found
* `run.status` - *string* suite's ending status
* `run.stats` - *object* various counts for this suite/chunk
* `run.stats.total` - *long* total number of tests
* `run.stats.bytes` - *long* total number of bytes in structured log
* `run.stats.error` - *long* total number of tests ending in error
* `run.stats.skip` - *long* total number of tests skipped
* `run.stats.pass` - *long* total number of tests that passed
* `run.stats.duration` - *seconds* duration of this suite 
* `run.stats.fail` - *long* total number of tests that end in failure
* `run.stats.end_time` - *timestamp* when the suite finished running
* `run.stats.none` - *long* total number of tests that did not end 
* `run.stats.start_time` - *timestamp* when the suite started running 
* `run.stats.lines` - *long* total number of lines in the structured log
* `run.stats.ok` - *long* number of tests where `result==expected`
* `run.stats.timeout` - *long* total number of tests that ended in timeout
* `run.stats.crash` - *long* total number of tests that ended in crash
* `run.talos` - *boolean* indicates if Talos performance results can be found in the text log
* `run.key` - *string* complicated buildbot string describing this run (and used to generate these other properties, so is redundant)
* `run.logurl` - *string* url to find the text log

`build` Columns
---------------

Properties describing the build 


* `build.product` - *string* name of software being built
* `build.id` - *uid* unique build id
* `build.branch` - *string* the branch in hg
* `build.revision` - *string* revision number found in hg
* `build.locale` - *string* locale option for build
* `build.type` - *string* more options
* `build.date` - *timestamp* when the build was assembled
* `build.release` - *string* ?not sure?
* `build.url` - *string* where you can find the build results
* `build.name` - *string* full name of the build, including options (parsed to create the other properties, so is redundant)


`machine` Columns
-----------------

Properties of the machine that ran the suite

* `machine.platform` - *string* type of machine 
* `machine.os` - *string* operating system 
* `machine.name` - *string* the particular piece of hardware 


`other` Columns
---------------

A catch-all where (new) properties missed by the ETL are recorded 

`etl` Columns
-------------

Markup from the ETL process

* `etl.id` - *integer* ETL uid for this record
* `etl.timestamp` - *timestamp* when the ETL was run 
* `etl.revision` - *string* git revision number of the ETL
* `etl.duration` - *seconds* - how long the ETL took
* `etl.source` - *object* another structure, just like this, which describes the parent ETL that fed this:  An effective audit chain.
* `etl.name` - *string* humane description of this ETL step 
* `etl.type` - *string* either `join` or `aggregation` indicating if this ETL step was adding or reducing information
