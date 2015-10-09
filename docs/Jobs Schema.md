
Jobs
====

The Jobs table records all BuildBot jobs, including the timings of the 
individual actions.  

`jobs` Columns
--------------

These can be found at the top of each logfile.  Use the `run` properties 
for finer grained querying

* `action.builder` - *string* full build name given to this job.  Same as 
  `run.key`
* `action.start_time` - *timestamp* name of the subtest
* `action.builduid` - *hex* some unique hex value
* `action.results` - *string* BuildBot end status; same as `run.buildbot_status`

`jobs.action.timings` Columns
----------------------------

All timing details can be had by querying `{"from":"jobs.action.timings"}`.  
Like everything else in Active Data, the various depth of timing has been 
denormalized for ease of querying.  Each property here is the short form; 
prefix with `action.timings.` for the absolute name.

* `order` - *integer* the number of this step for showing the order they 
  happened
* `builder` - *object* properties for the BuildBot step 
  * `builder.step` - *string* name of the step 
  * `builder.status` - *string* the BuildBot status code for this step
  * `builder.parts` - *string* some steps, like `set props` have a variable 
  number of parameters, which can be found here
  * `builder.start_time` - *timestamp* when the step started
  * `builder.end_time` - *timestamp* when the step ended
  * `builder.duration` - *seconds* total length of time for this step.  It 
  may be longer than what start/end would indicate because it includes the 
  inter-builder step time
  * `builder.elapsedTime` - Before the end of each BuildBot step the is an 
  `elapsedTime=` line; which is the script reporting it is done.  This can 
  be much shorter than the `duration`, if BuildBot is acting slow.
* `harness` - *object* properties for the mozharness steps
  * `harness.step` - *string* name of the step 
  * `harness.mode` - *string* indicate if `running` or `skipped` or other 
  * `harness.start_time` - *timestamp* when the step started
  * `harness.end_time` - *timestamp* when the step ended
  * `harness.duration` - *seconds* total length of time for this step. 

`run` Columns
-------------

Properties that describe the run of this job.

* `run.suite` - *string* name of the suite
* `run.chunk` - *integer* each suite is broken into chucks to parallelize the 
run, each chunk is given a number
* `run.type` - *string* either "e10s" or `null`
* `run.buildbot_status` - *string* BuildBot's end status
* `run.timestamp` - *timestamp*
* `run.job_number` - *string*
* `run.files` - *array* of files recorded for this suite, one or more of which  
are the structured log digested to make this record
	* `run.files.name` - *string* name of the file
	* `run.files.url` - *string* url where he contents can/could be found
* `run.machine` - *object* properties of the machine that ran the suite
	* `machine.os` - *string* operating system
	* `machine.name` - *string* the particular piece of hardware that ran suite
* `run.talos` - *boolean* indicates if Talos performance results can be found 
in the text log
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
* `build.name` - *string* full name of the build, including options (parsed 
  to create the other properties, so is redundant)

`repo` Columns
---------------

Properties of the changeset, revision and push

* `repo.changeset` - *object*
	* `repo.changeset.id` - *string* unique hash value of changeset
    * `repo.changeset.files` - *strings* full path to files changed
	* `repo.changeset.date` - *timestamp*
	* `repo.changeset.description` - *string* text assigned to push *This 
    property has been parsed into words*
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
* `etl.source` - *object* another structure, just like this, which describes 
the parent ETL that fed this:  An effective audit chain.
* `etl.name` - *string* humane description of this ETL step
* `etl.type` - *string* either `join` or `aggregation` indicating if this ETL step was adding or reducing information
