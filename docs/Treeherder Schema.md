
Treeherder Schema
=================

[Treeherder](https://wiki.mozilla.org/EngineeringProductivity/Projects/Treeherder) tracks code changes, and the jobs (tests) run against those changes.  This table is an extract of all Treeherder's data,  ActiveData's ETL pipeline annotates Buildbot jobs and TaskCluster tasks with Treeherder data, and use

`task` Columns
--------------

* `id` - *string* the TaskCluster uid 



`machine` Columns
-----------------

* `os` - *string*  
* `name` - *string*
* `pool` - *string*
* `platform` - *string*

`job` Columns
-------------




        "job": {
                "build_system_type": {
         
`job.notes` 
-----------                
                "notes": {
* `note.note` - *string*
* `note.failure_classification` - *string*
* `note.bug_id` - *string*
* `note.revision` - *string*
* `note.timestamp` - *string*
* `note.who` - *string*


},
                "build": {
                    "properties": {
                        "os": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": true
                        },
                        "type": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": true
                        },
                        "platform": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": true
                        },
                        "architecture": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": true
                        }
                    }
                },
                "ref_data_name": {
                    "index": "not_analyzed",
                    "type": "string",
                    "doc_values": true
                },
                "repo": {
                    "properties": {
                        "revision12": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": true
                        },
                        "branch": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": true
                        },
                        "revision": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": true
                        }
                    }
                },
                "details": {
                    "type": "nested",
                    "properties": {
                        "title": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": true
                        },
                        "value": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": true
                        },
                        "url": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": true
                        }
                    }
                },
                "stars": {
                    "type": "nested",
                    "properties": {
                        "bug_id": {
                            "type": "long",
                            "doc_values": true
                        },
                        "who": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": true
                        }
                    }
                },
                "etl": {
                    "properties": {
                        "id": {
                            "type": "integer",
                            "doc_values": true
                        },
                        "source": {
                            "properties": {
                                "id": {
                                    "type": "integer",
                                    "doc_values": true
                                },
                                "source": {
                                    "properties": {
                                        "id": {
                                            "type": "integer",
                                            "doc_values": true
                                        },
                                        "source": {
                                            "properties": {
                                                "id": {
                                                    "type": "integer",
                                                    "doc_values": true
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        "timestamp": {
                            "type": "double",
                            "doc_values": true
                        }
                    }
                },
                "job": {
                    "properties": {
                        "result": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": true
                        },
                        "reason": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": true
                        },
                        "tier": {
                            "type": "long",
                            "doc_values": true
                        },
                        "timing": {
                            "properties": {
                                "submit": {
                                    "type": "double",
                                    "doc_values": true
                                },
                                "start": {
                                    "type": "double",
                                    "doc_values": true
                                },
                                "end": {
                                    "type": "double",
                                    "doc_values": true
                                },
                                "last_modified": {
                                    "type": "double",
                                    "doc_values": true
                                }
                            }
                        },
                        "failure_classification": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": true
                        },
                        "guid": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": true
                        },
                        "id": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": true
                        },
                        "state": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": true
                        },
                        "type": {
                            "properties": {
                                "symbol": {
                                    "index": "not_analyzed",
                                    "type": "string",
                                    "doc_values": true
                                },
                                "name": {
                                    "index": "not_analyzed",
                                    "type": "string",
                                    "doc_values": true
                                },
                                "description": {
                                    "index": "not_analyzed",
                                    "type": "string",
                                    "doc_values": true
                                }
                            }
                        },
                        "group": {
                            "properties": {
                                "symbol": {
                                    "index": "not_analyzed",
                                    "type": "string",
                                    "doc_values": true
                                },
                                "name": {
                                    "index": "not_analyzed",
                                    "type": "string",
                                    "doc_values": true
                                },
                                "description": {
                                    "index": "not_analyzed",
                                    "type": "string",
                                    "doc_values": true
                                }
                            }
                        },
                        "who": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": true
                        }
                    }
                }
            }
        }
    },
    "aliases": [
        "treeherder"
    ]

}





`task` Columns
--------------

* `id` - *string* uid for this task
* `group.id` - *string* - all tasks belong to a group, connected by a dependency DAG
* `dependencies` - *strings* list of task ids that this task depends on
* `command` - *string* command-line string to start the task
* `worker.type` - *string* canonical name given to this worker
* `worker.group` - *string* where the worker is located
* `worker.id` - *string* name given to the worker machine
* `retries.total` - *integer* number of retires before this task is dead
* `retries.remaining` - *integer* the number of remaining retries
* `runs` - *array of objects* list of multiple other runs including this one
* `created` - *timestamp* when this task was created 
* `deadline` - *timestamp* when this task should have ended
* `expires` - *timestamp* - when this task will be forgotten
* `scheduler.id` - *string* name of automation 
* `provisioner.id` - *string* name of the automation that started the task
* `routes` - *array of strings* list of access paths to help find things 

`task.artifacts` Columns
------------------------

Every task can generate multiple artifacts.  This is a nested array, so you can query it directly (eg `{"from": "task.task.artifacts"}`)

* `name` - *string* short name for the artifact
* `url` - *URL* points to the content of the artifact
* `contentType` - *string* what to expect should you load the content
* `expires` - *timestamp* when this artifact is no longer expected to exist
* `storageType` - *string* usually "s3"

`task.tags` Columns
-------------------

TaskCluster allows the worker to annotate the task metatdata. This is good, but does result in a non-standard inner property names found in `metadata` or `extra`, or `tags`, or any two, or all three.  All these properties are flattened into `{"name":name, "value":value}` form, and put in the `task.tags` array.  Where `name` is the full path the the primitive JSON `value`.

For example, 
	
	{"extra":{
		"suite":{
			"name":"mochitest", 
			"flavor":"browser-chrome-chunked"
		}
	}}

is converted to 

    {"task":{"tags":[
		{"name":"suite.flavor","value":"browser-chrome-chunked"},
		{"name":"suite.name","value":"mochitest"}
	]}
					


`run` Columns
-------------

Properties describing the running task. A copy of other task properties in an attempt to standardize the names of values.  

* `run.suite` - *object* description of the test suite, if the task is running one
    * `run.suite.name` - *string* name of the suite
    * `run.suite.flavor` - *string* variation on the suite
* `run.chunk` - *integer* each suite is broken into chucks to parallelize the run, each chunk is given a number
* `run.machine` - *object* properties of the machine that ran the suite
	* `machine.platform` - *string* high-level platform category
	* `machine.name` - *string* the particular piece of hardware that ran suite

`build` Columns
---------------

Properties describing the build.  A copy of task properties in an attempt to standardize the names of values.

* `build.product` - *string* name of software being built
* `build.platform` - *string* platform that's targeted by build
* `build.branch` - *string* the branch in hg
* `build.revision` - *string* revision number found in hg
* `build.revision12` - *string* first 12 characters of revision number
* `build.type` - *string* more options
* `build.url` - *string* where you can find the build results

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
