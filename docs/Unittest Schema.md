
Unittest Logs
=============

The Unittest logs are deep JSON structures describing each of the individual test results.  There are seven major properties.

`result` Columns
----------------

Describes properties exclusive to an individual test result

* `result.test` - *string* path and filename of the specific test
* `result.status` - *string* recording the test result
* ~~`result.result` - *string* recording the test result~~
* `result.expected` - *string* representing what was expected of the test.  Not all tests are expected to pass.
* `result.ok` - *boolean* where `result==expected`, and all subtests too
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

`result.subtests` Columns
-------------------------

Some test types have multiple individual results per top-level test, these are represented as *subtests*.  To query the subtests use `{"from":"unittest.result.subtests"}`

* `result.subtests.ordering` - *integer* for maintaining the original log ordering
* `result.subtests.name` - *string* name of the subtest
* ~~`result.subtests.subtest` - *string* name of the subtest~~
* `result.subtests.ok` - *boolean* `true` iff `status==expected``
* `result.subtests.status` - *string* result of the subtest
* `result.subtests.expected` - *string* the expected status
* `result.subtests.message` - *string* some description
* `result.subtests.timestamp` - *timestamp* given to this log line


`run` Columns
-------------

Properties that describe the run of this test suite, and the many tests that are contained within.  Most are verbatim from the Pulse messages received by the ETL, and replicated for every test result in the data store.  **If you are interested in suite-level aggregates, be sure to filter by the suite's canonical test result: `"where": {"eq":{"etl.id":0}}`**

* `run.suite` - *string* name of the suite
* `run.chunk` - *integer* each suite is broken into chucks to parallelize the run, each chunk is given a number
* `run.type` - *string* either "e10s" or `null`
* ~~`run.insertion_time` - *timestamp* time inserted into the `exchange/build/normalized` Pulse exchange~~
* ~~`run.status` - *integer* buildbot's end status~~
* `run.buildbot_status` - *string* buildbot's end status
* `run.timestamp` - *timestamp*
* `run.job_number` - *string*
* `run.files` - *array* of files recorded for this suite, one or more of which are the structured log digested to make this record
	* `run.files.name` - *string* name of the file
	* `run.files.url` - *string* url where he contents can/could be found
* `run.machine` - *object* properties of the machine that ran the suite
	* `machine.os` - *string* operating system
	* `machine.name` - *string* the particular piece of hardware that ran suite
* `run.stats` - *object* various counts for this suite/chunk
	* `run.stats.ok` - *long* number of tests where `result==expected`
	* `run.stats.pass` - *long* number of tests that passed
	* `run.stats.fail` - *long* number of tests that end in failure
	* `run.stats.error` - *long* number of tests ending in error
	* `run.stats.skip` - *long* number of tests skipped
	* `run.stats.none` - *long* number of tests that did not end
	* `run.stats.timeout` - *long* number of tests that ended in timeout
	* `run.stats.crash` - *long* number of tests that ended in crash
	* `run.stats.total` - *long* number of tests
	* `run.stats.start_time` - *timestamp* when the suite started running
	* `run.stats.end_time` - *timestamp* when the suite finished running
	* `run.stats.duration` - *seconds* duration of this suite
	* `run.stats.bytes` - *long* number of bytes in structured log
	* `run.stats.lines` - *long* number of lines in the structured log
* `run.talos` - *boolean* indicates if Talos performance results can be found in the text log
* `run.logurl` - *string* url to find the text log
* `run.key` - *string* created by PulseTranslator to represent these other properties

`build` Columns
---------------

Properties describing the build

* `build.product` - *string* name of software being built
* `build.platform` - *string* platform that's targeted by build
* `build.id` - *uid* unique build id
* `build.branch` - *string* the branch in hg
* `build.revision` - *string* revision number found in hg
* `build.revision12` - *string* first 12 characters of revision number
* `build.locale` - *string* locale option for build
* `build.type` - *string* more options
* `build.date` - *timestamp* when the build was assembled
* `build.release` - *string* ?not sure?
* `build.url` - *string* where you can find the build results
* `build.name` - *string* full name of the build, including options (parsed to create the other properties, so is redundant)

`repo` Columns
---------------

Properties of the changeset, revision and push

* `repo.changeset` - *object*
	* `repo.changeset.id` - *string* unique hash value of changeset
    * `repo.changeset.files` - *strings* full path to files changed
	* `repo.changeset.date` - *timestamp*
	* `repo.changeset.description` - *string* text assigned to push *This property has been parsed into words
	* `repo.changeset.author` - *string* author info
* `repo.index` - *integer* unique value given to this revision by hg.mozilla.org
* `repo.branch` - *string* name of the branch
* `repo.push` - *object* more about the push
    * `repo.push.id` - *integer* unique value given to this push by hg.mozilla.org
    * `repo.push.user` - *string* name of person doing push
    * `repo.push.date` - *timestamp* when push was applied
* `repo.parents` - *strings* ids of any parents
* `repo.children` - *strings* ids of any children


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
