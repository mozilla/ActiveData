
Perf Logs
=============

The (Talos) performance data is in the `perf` table.


`result` Columns
----------------

Describes properties exclusive to an individual test result

* `result.test` - *string* name of the specific test in a suite
* `result.ordering` - *integer* the index of the test in the original result object
* `result.samples` - *array` ordered list of individual replicates
* `result.stats` - *object* multiple statistics of the sample, for convenience
  * `result.stats.count` - *integer* 
  * `result.stats.mean` - *number* 
  * `result.stats.median` - *number* 
  * `result.stats.std` - *number* 
  * `result.stats.variance` - *number* 
  * `result.stats.skew` - *integer* 
  * `result.stats.kurtosis` - *integer* 
  * `result.stats.min` - *number* 
  * `result.stats.max` - *number* 
  * `result.stats.last` - *number* the first sample
  * `result.stats.first` - *number* the last sample
  * `result.stats.s0` - *number* count of samples
  * `result.stats.s1` - *number* sum
  * `result.stats.s2` - *number* sum-of-squares
  * `result.stats.s3` - *number* sum-of-cubes
  * `result.stats.s4` - *number* sum-of-hypercube
 

`run` Columns
-------------

Properties that describe the run of this test suite, and the many tests that are contained within. 

* `run.suite` - *string* name of the suite
* `run.buildbot_status` - *string* buildbot's end status
* `run.timestamp` - *timestamp*
* `run.job_number` - *string*
* `run.files` - *array* of files recorded for this suite, one or more of which are the structured log digested to make this record
	* `run.files.name` - *string* name of the file
	* `run.files.url` - *string* url where he contents can/could be found
* `run.machine` - *object* properties of the machine that ran the suite
	* `machine.os` - *string* operating system
	* `machine.name` - *string* the particular piece of hardware that ran suite
* `run.stats` - *object* the geometric statistics for the median of all tests in this suite
* `run.talos` - *boolean* indicates if Talos performance results can be found in the text log
* `run.logurl` - *string* URL to find the text log
* `run.key` - *string* created by PulseTranslator to represent these other properties
* `run.insertion_time` - *timestamp* when this job was requested


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
* `etl.source` - *object* another structure, just like this, which describes the parent ETL that fed this: An effective audit chain.
* `etl.name` - *string* humane description of this ETL step
* `etl.type` - *string* either `join` or `aggregation` indicating if this ETL step was adding or reducing information
