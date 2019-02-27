
# All Schemas

A semi-automated extract of the ActiveData schemas as of 2019-02-27

## activedata_requests
    {
        "content_length": {"type": "long"},
        "data": {"type": "text"},
        "error": {
            "enabled": false,
            "type": "object"
        },
        "from": {"type": "keyword"},
        "http_accept_encoding": {"type": "keyword"},
        "http_user_agent": {"type": "keyword"},
        "path": {"type": "keyword"},
        "query": {
            "enabled": false,
            "type": "object"
        },
        "query_text": {"type": "text"},
        "remote_addr": {"type": "keyword"},
        "timestamp": {"type": "double"}
    }
## task
    {
        "action": {
            "dynamic": "true",
            "properties": {
                "duration": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "end_time": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "etl": {
                    "dynamic": "true",
                    "properties": {
                        "total_bytes": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "start_time": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "timings": {
                    "dynamic": "true",
                    "properties": {
                        "~N~": {
                            "dynamic": "true",
                            "properties": {
                                "duration": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "end_time": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "harness": {
                                    "dynamic": "true",
                                    "properties": {
                                        "duration": {
                                            "dynamic": "true",
                                            "properties": {"~n~": {
                                                "store": true,
                                                "type": "double"
                                            }}
                                        },
                                        "end_time": {
                                            "dynamic": "true",
                                            "properties": {"~n~": {
                                                "store": true,
                                                "type": "double"
                                            }}
                                        },
                                        "mode": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "result": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "start_time": {
                                            "dynamic": "true",
                                            "properties": {"~n~": {
                                                "store": true,
                                                "type": "double"
                                            }}
                                        },
                                        "step": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "~e~": {
                                            "store": true,
                                            "type": "long"
                                        }
                                    }
                                },
                                "order": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "start_time": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "step": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            },
                            "type": "nested"
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "type": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "build": {
            "dynamic": "true",
            "properties": {
                "action": {
                    "dynamic": "true",
                    "properties": {
                        "duration": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "end_time": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "etl": {
                            "dynamic": "true",
                            "properties": {
                                "total_bytes": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "start_time": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "type": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "branch": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "build": {
                    "dynamic": "true",
                    "properties": {
                        "branch": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "date": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "platform": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "product": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "revision": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "revision12": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "task": {
                            "dynamic": "true",
                            "properties": {"~e~": {
                                "store": true,
                                "type": "long"
                            }}
                        },
                        "train": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "trigger": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "type": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "version": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "channel": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "compiler": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "date": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "id": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "platform": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "product": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "repo": {
                    "dynamic": "true",
                    "properties": {
                        "branch": {
                            "dynamic": "true",
                            "properties": {
                                "locale": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "name": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "url": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "changeset": {
                            "dynamic": "true",
                            "properties": {
                                "author": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "backedoutby": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "bug": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "date": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "description": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "id": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "id12": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "index": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "push": {
                            "dynamic": "true",
                            "properties": {
                                "date": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "id": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "user": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "rev": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "tags": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "revision": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "revision12": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "run": {
                    "dynamic": "true",
                    "properties": {
                        "key": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "machine": {
                            "dynamic": "true",
                            "properties": {
                                "aws": {
                                    "dynamic": "true",
                                    "properties": {
                                        "instance_type": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "~e~": {
                                            "store": true,
                                            "type": "long"
                                        }
                                    }
                                },
                                "aws_instance_type": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "platform": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "tc_worker_type": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "name": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "suite": {
                            "dynamic": "true",
                            "properties": {"~e~": {
                                "store": true,
                                "type": "long"
                            }}
                        },
                        "timestamp": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "task": {
                    "dynamic": "true",
                    "properties": {
                        "created": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "deadline": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "dependencies": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "expires": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "features": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "group": {
                            "dynamic": "true",
                            "properties": {
                                "id": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "id": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "image": {
                            "dynamic": "true",
                            "properties": {
                                "path": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "taskId": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "type": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "kind": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "maxRunTime": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "mounts": {
                            "dynamic": "true",
                            "type": "object"
                        },
                        "parent": {
                            "dynamic": "true",
                            "properties": {
                                "id": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "priority": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "provisioner": {
                            "dynamic": "true",
                            "properties": {
                                "id": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "requires": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "retries": {
                            "dynamic": "true",
                            "properties": {
                                "remaining": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "total": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "run": {
                            "dynamic": "true",
                            "properties": {
                                "duration": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "end_time": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "reason_created": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "scheduled": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "start_time": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "state": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "status": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "worker": {
                                    "dynamic": "true",
                                    "properties": {
                                        "group": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "id": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "~e~": {
                                            "store": true,
                                            "type": "long"
                                        }
                                    }
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "scheduler": {
                            "dynamic": "true",
                            "properties": {
                                "id": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "signing": {
                            "dynamic": "true",
                            "properties": {
                                "cert": {
                                    "dynamic": "true",
                                    "type": "object"
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "state": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "version": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "worker": {
                            "dynamic": "true",
                            "properties": {
                                "group": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "id": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "type": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "train": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "treeherder": {
                    "dynamic": "true",
                    "properties": {
                        "build": {
                            "dynamic": "true",
                            "properties": {
                                "platform": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "collection": {
                            "dynamic": "true",
                            "properties": {
                                "asan": {
                                    "dynamic": "true",
                                    "properties": {"~b~": {
                                        "store": true,
                                        "type": "boolean"
                                    }}
                                },
                                "ccov": {
                                    "dynamic": "true",
                                    "properties": {"~b~": {
                                        "store": true,
                                        "type": "boolean"
                                    }}
                                },
                                "debug": {
                                    "dynamic": "true",
                                    "properties": {"~b~": {
                                        "store": true,
                                        "type": "boolean"
                                    }}
                                },
                                "fips": {
                                    "dynamic": "true",
                                    "properties": {"~b~": {
                                        "store": true,
                                        "type": "boolean"
                                    }}
                                },
                                "fuzz": {
                                    "dynamic": "true",
                                    "properties": {"~b~": {
                                        "store": true,
                                        "type": "boolean"
                                    }}
                                },
                                "make": {
                                    "dynamic": "true",
                                    "properties": {"~b~": {
                                        "store": true,
                                        "type": "boolean"
                                    }}
                                },
                                "opt": {
                                    "dynamic": "true",
                                    "properties": {"~b~": {
                                        "store": true,
                                        "type": "boolean"
                                    }}
                                },
                                "pgo": {
                                    "dynamic": "true",
                                    "properties": {"~b~": {
                                        "store": true,
                                        "type": "boolean"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "groupName": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "groupSymbol": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "jobKind": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "machine": {
                            "dynamic": "true",
                            "properties": {
                                "platform": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "symbol": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "tier": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "trigger": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "type": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "url": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "version": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "worker": {
                    "dynamic": "true",
                    "properties": {"~e~": {
                        "store": true,
                        "type": "long"
                    }}
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "etl": {
            "dynamic": "true",
            "properties": {
                "error": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "id": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "machine": {
                    "dynamic": "true",
                    "properties": {
                        "name": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "os": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "pid": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "python": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "source": {
                    "dynamic": "true",
                    "properties": {
                        "bucket": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "id": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "name": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "source": {
                            "dynamic": "true",
                            "properties": {
                                "count": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "id": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "source": {
                                    "dynamic": "true",
                                    "properties": {
                                        "id": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "~e~": {
                                            "store": true,
                                            "type": "long"
                                        }
                                    }
                                },
                                "type": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "timestamp": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "type": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "timestamp": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "type": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "repo": {
            "dynamic": "true",
            "properties": {
                "branch": {
                    "dynamic": "true",
                    "properties": {
                        "locale": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "name": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "url": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "changeset": {
                    "dynamic": "true",
                    "properties": {
                        "author": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "backedoutby": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "bug": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "date": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "description": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "id": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "id12": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "moves": {
                            "dynamic": "true",
                            "type": "object"
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "error": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "index": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "landingsystem": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "push": {
                    "dynamic": "true",
                    "properties": {
                        "date": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "id": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "user": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "rev": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "tags": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "run": {
            "dynamic": "true",
            "properties": {
                "aws": {
                    "dynamic": "true",
                    "properties": {
                        "ami-id": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "availability-zone": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "instance-id": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "instance-type": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "local-ipv4": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "public-hostname": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "public-ipv4": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "browser": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "chunk": {
                    "dynamic": "true",
                    "properties": {
                        "~n~": {
                            "store": true,
                            "type": "double"
                        },
                        "~s~": {
                            "store": true,
                            "type": "keyword"
                        }
                    }
                },
                "config": {
                    "dynamic": "true",
                    "properties": {
                        "deploymentId": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "runTaskAsCurrentUser": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "runTasksAsCurrentUser": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "framework": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "generic-worker": {
                    "dynamic": "true",
                    "properties": {
                        "go-arch": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "go-os": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "go-version": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "release": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "revision": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "source": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "version": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "key": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "machine": {
                    "dynamic": "true",
                    "properties": {
                        "architecture": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "aws": {
                            "dynamic": "true",
                            "properties": {
                                "instance_type": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "aws_instance_type": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "os": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "platform": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "platfrom": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "tc_worker_type": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "machine-setup": {
                    "dynamic": "true",
                    "properties": {
                        "ami-created": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "created": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "info": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "maintainer": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "manifest": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "note": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "rollback": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "script": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "name": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "suite": {
                    "dynamic": "true",
                    "properties": {
                        "flavor": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "fullname": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "name": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "timestamp": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "trigger": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "type": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "task": {
            "dynamic": "true",
            "properties": {
                "artifacts": {
                    "dynamic": "true",
                    "properties": {
                        "~N~": {
                            "dynamic": "true",
                            "properties": {
                                "contentType": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "ehpires": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "expires": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "name": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "storageType": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "url": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            },
                            "type": "nested"
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "capabilities": {
                    "dynamic": "true",
                    "properties": {
                        "devices": {
                            "dynamic": "true",
                            "properties": {
                                "loopbackAudio": {
                                    "dynamic": "true",
                                    "properties": {"~b~": {
                                        "store": true,
                                        "type": "boolean"
                                    }}
                                },
                                "loopbackVideo": {
                                    "dynamic": "true",
                                    "properties": {"~b~": {
                                        "store": true,
                                        "type": "boolean"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "privileged": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "command": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "created": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "deadline": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "dependencies": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "env": {
                    "dynamic": "true",
                    "properties": {
                        "~N~": {
                            "dynamic": "true",
                            "properties": {
                                "name": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "value": {
                                    "dynamic": "true",
                                    "properties": {
                                        "ANALYSIS_ID": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "ANALYSIS_SOURCE": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "deadline": {"properties": {"~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }}},
                                        "expires": {"properties": {"~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }}},
                                        "provisionerId": {"properties": {"~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }}},
                                        "retriesLeft": {"properties": {"~n~": {
                                            "store": true,
                                            "type": "double"
                                        }}},
                                        "runs": {"properties": {
                                            "~N~": {
                                                "properties": {
                                                    "reasonCreated": {"properties": {"~s~": {
                                                        "store": true,
                                                        "type": "keyword"
                                                    }}},
                                                    "reasonResolved": {"properties": {"~s~": {
                                                        "store": true,
                                                        "type": "keyword"
                                                    }}},
                                                    "resolved": {"properties": {"~s~": {
                                                        "store": true,
                                                        "type": "keyword"
                                                    }}},
                                                    "runId": {"properties": {"~n~": {
                                                        "store": true,
                                                        "type": "double"
                                                    }}},
                                                    "scheduled": {"properties": {"~s~": {
                                                        "store": true,
                                                        "type": "keyword"
                                                    }}},
                                                    "started": {"properties": {"~s~": {
                                                        "store": true,
                                                        "type": "keyword"
                                                    }}},
                                                    "state": {"properties": {"~s~": {
                                                        "store": true,
                                                        "type": "keyword"
                                                    }}},
                                                    "takenUntil": {"properties": {"~s~": {
                                                        "store": true,
                                                        "type": "keyword"
                                                    }}},
                                                    "workerGroup": {"properties": {"~s~": {
                                                        "store": true,
                                                        "type": "keyword"
                                                    }}},
                                                    "workerId": {"properties": {"~s~": {
                                                        "store": true,
                                                        "type": "keyword"
                                                    }}},
                                                    "~e~": {
                                                        "store": true,
                                                        "type": "long"
                                                    }
                                                },
                                                "type": "nested"
                                            },
                                            "~e~": {
                                                "store": true,
                                                "type": "long"
                                            }
                                        }},
                                        "schedulerId": {"properties": {"~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }}},
                                        "state": {"properties": {"~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }}},
                                        "task-reference": {"properties": {"~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }}},
                                        "taskGroupId": {"properties": {"~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }}},
                                        "taskId": {"properties": {"~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }}},
                                        "workerType": {"properties": {"~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }}},
                                        "~b~": {
                                            "store": true,
                                            "type": "boolean"
                                        },
                                        "~e~": {
                                            "store": true,
                                            "type": "long"
                                        },
                                        "~n~": {
                                            "store": true,
                                            "type": "double"
                                        },
                                        "~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }
                                    }
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            },
                            "type": "nested"
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "expires": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "features": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "group": {
                    "dynamic": "true",
                    "properties": {
                        "id": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "id": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "image": {
                    "dynamic": "true",
                    "properties": {
                        "artifact": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "name": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "namespace": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "path": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "repository": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "runId": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "tag": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "taskId": {
                            "dynamic": "true",
                            "properties": {
                                "task-reference": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                },
                                "~s~": {
                                    "store": true,
                                    "type": "keyword"
                                }
                            }
                        },
                        "type": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "kind": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "maxRunTime": {
                    "dynamic": "true",
                    "properties": {
                        "~n~": {
                            "store": true,
                            "type": "double"
                        },
                        "~s~": {
                            "store": true,
                            "type": "keyword"
                        }
                    }
                },
                "mounts": {
                    "dynamic": "true",
                    "properties": {
                        "~N~": {
                            "dynamic": "true",
                            "properties": {
                                "cacheName": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "content": {
                                    "dynamic": "true",
                                    "properties": {
                                        "artifact": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "base64": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "file": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "raw": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "sha256": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "sha356": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "task-id": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "taskId": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "url": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "~e~": {
                                            "store": true,
                                            "type": "long"
                                        }
                                    }
                                },
                                "director": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "directory": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "file": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "format": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            },
                            "type": "nested"
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "parent": {
                    "dynamic": "true",
                    "properties": {
                        "id": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "priority": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "provisioner": {
                    "dynamic": "true",
                    "properties": {
                        "id": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "reboot": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "requires": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "retries": {
                    "dynamic": "true",
                    "properties": {
                        "remaining": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "total": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "routes": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "run": {
                    "dynamic": "true",
                    "properties": {
                        "duration": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "end_time": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "reason_created": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "scheduled": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "start_time": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "state": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "status": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "worker": {
                            "dynamic": "true",
                            "properties": {
                                "group": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "id": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "runs": {
                    "dynamic": "true",
                    "properties": {
                        "~N~": {
                            "dynamic": "true",
                            "properties": {
                                "duration": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "end_time": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "reason_created": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "scheduled": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "start_time": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "state": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "status": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "worker": {
                                    "dynamic": "true",
                                    "properties": {
                                        "group": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "id": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "~e~": {
                                            "store": true,
                                            "type": "long"
                                        }
                                    }
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            },
                            "type": "nested"
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "scheduler": {
                    "dynamic": "true",
                    "properties": {
                        "id": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "scopes": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "signing": {
                    "dynamic": "true",
                    "properties": {
                        "cert": {
                            "dynamic": "true",
                            "type": "object"
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "state": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "tags": {
                    "dynamic": "true",
                    "properties": {
                        "~N~": {
                            "dynamic": "true",
                            "properties": {
                                "name": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "value": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            },
                            "type": "nested"
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "version": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "worker": {
                    "dynamic": "true",
                    "properties": {
                        "group": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "id": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "type": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "treeherder": {
            "dynamic": "true",
            "properties": {
                "build": {
                    "dynamic": "true",
                    "properties": {
                        "platform": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "collection": {
                    "dynamic": "true",
                    "properties": {
                        "32": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "64": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "7,0/x86_64/debug": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "7,0/x86_64/opt": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "7-0/x86_64/debug": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "7-0/x86_64/opt": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "86": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "all": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "asan": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "ccov": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "cfi": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "debug": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "fips": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "fuzz": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "lto": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "make": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "opt": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "opt/opt": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "opt/opt/opt": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "opt/opt/opt/opt": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "opt/opt/opt/opt/opt": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "pgo": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "pgo/opt": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "pgo/opt/opt": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "release": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "groupName": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "groupSymbol": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "jobKind": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "jobSymbol": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "labels": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "machine": {
                    "dynamic": "true",
                    "properties": {
                        "architecture": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "os": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "platform": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "platfrom": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "productName": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "symbol": {
                    "dynamic": "true",
                    "properties": {
                        "~n~": {
                            "store": true,
                            "type": "double"
                        },
                        "~s~": {
                            "store": true,
                            "type": "keyword"
                        }
                    }
                },
                "tier": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "~e~": {
            "store": true,
            "type": "long"
        }
    }
## firefox-files
    {
        "bug": {"properties": {
            "component": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "product": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "~e~": {
                "store": true,
                "type": "long"
            }
        }},
        "etl": {"properties": {
            "id": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "source": {"properties": {
                "id": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "machine": {"properties": {
                    "name": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "os": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "pid": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "python": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "name": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "revision": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "source": {"properties": {
                    "id": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "machine": {"properties": {
                        "name": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "os": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "pid": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "python": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }},
                    "source": {"properties": {
                        "bucket": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "id": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "name": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "source": {"properties": {
                            "count": {"properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}},
                            "id": {"properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}},
                            "source": {"properties": {
                                "id": {"properties": {"~s~": {
                                    "store": true,
                                    "type": "keyword"
                                }}},
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }},
                            "type": {"properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}},
                            "~e~": {
                                "store": true,
                                "type": "long"
                            }
                        }},
                        "timestamp": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "type": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }},
                    "timestamp": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "type": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "timestamp": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "type": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "url": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }},
            "type": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "~e~": {
                "store": true,
                "type": "long"
            }
        }},
        "file": {"properties": {
            "full_name": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "name": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "path": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "type": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "~e~": {
                "store": true,
                "type": "long"
            }
        }},
        "repo": {
            "dynamic": "true",
            "properties": {
                "changeset": {
                    "dynamic": "true",
                    "properties": {
                        "author": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "backedoutby": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "bug": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "date": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "id": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "id12": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "index": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "landingsystem": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "rev": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "~e~": {
            "store": true,
            "type": "long"
        }
    }
## debug_active_data
    null
## saved_queries
    {
        "create_time": {
            "store": true,
            "type": "double"
        },
        "hash": {
            "store": true,
            "type": "keyword"
        },
        "last_used": {
            "store": true,
            "type": "double"
        },
        "query": {
            "store": true,
            "type": "text"
        }
    }
## unittest
    {
        "action": {
            "dynamic": "true",
            "properties": {
                "duration": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "end_time": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "start_time": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "type": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "build": {
            "dynamic": "true",
            "properties": {
                "branch": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "date": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "platform": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "product": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "revision": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "revision12": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "task": {"properties": {"~e~": {
                    "store": true,
                    "type": "long"
                }}},
                "train": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "type": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "url": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "etl": {"properties": {
            "duration": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "id": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "machine": {"properties": {
                "name": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "os": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "pid": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "python": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }},
            "name": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "revision": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "source": {"properties": {
                "id": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "machine": {"properties": {
                    "name": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "os": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "pid": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "python": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "name": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "revision": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "source": {"properties": {
                    "id": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "machine": {"properties": {
                        "name": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "os": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "pid": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "python": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }},
                    "source": {"properties": {
                        "bucket": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "id": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "name": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "source": {"properties": {
                            "count": {"properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}},
                            "id": {"properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}},
                            "source": {"properties": {
                                "id": {"properties": {"~s~": {
                                    "store": true,
                                    "type": "keyword"
                                }}},
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }},
                            "type": {"properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}},
                            "~e~": {
                                "store": true,
                                "type": "long"
                            }
                        }},
                        "timestamp": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "type": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }},
                    "timestamp": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "type": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "timestamp": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "type": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "url": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }},
            "timestamp": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "type": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "~e~": {
                "store": true,
                "type": "long"
            }
        }},
        "repo": {
            "dynamic": "true",
            "properties": {
                "branch": {
                    "dynamic": "true",
                    "properties": {
                        "locale": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "name": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "url": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "changeset": {
                    "dynamic": "true",
                    "properties": {
                        "author": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "backedoutby": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "bug": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "date": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "description": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "id": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "id12": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "error": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "index": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "landingsystem": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "push": {
                    "dynamic": "true",
                    "properties": {
                        "date": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "id": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "user": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "rev": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "result": {
            "dynamic": "true",
            "properties": {
                "crash": {
                    "dynamic": "true",
                    "properties": {"~b~": {
                        "store": true,
                        "type": "boolean"
                    }}
                },
                "duration": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "end_time": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "expected": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "last_log_time": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "missing_subtests": {
                    "dynamic": "true",
                    "properties": {"~b~": {
                        "store": true,
                        "type": "boolean"
                    }}
                },
                "missing_test_end": {
                    "dynamic": "true",
                    "properties": {"~b~": {
                        "store": true,
                        "type": "boolean"
                    }}
                },
                "missing_test_start": {
                    "dynamic": "true",
                    "properties": {"~b~": {
                        "store": true,
                        "type": "boolean"
                    }}
                },
                "ok": {
                    "dynamic": "true",
                    "properties": {"~b~": {
                        "store": true,
                        "type": "boolean"
                    }}
                },
                "result": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "start_time": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "stats": {
                    "dynamic": "true",
                    "properties": {
                        "action": {
                            "dynamic": "true",
                            "properties": {
                                "test_status": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "error": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "fail": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "notrun": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "pass": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "skip": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "timeout": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "status": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "subtests": {
                    "dynamic": "true",
                    "properties": {
                        "~N~": {
                            "dynamic": "true",
                            "properties": {
                                "expected": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "message": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "name": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "ok": {
                                    "dynamic": "true",
                                    "properties": {"~b~": {
                                        "store": true,
                                        "type": "boolean"
                                    }}
                                },
                                "ordering": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "repeat": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "status": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "subtest": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "timestamp": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            },
                            "type": "nested"
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "test": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "run": {
            "dynamic": "true",
            "properties": {
                "aws": {
                    "dynamic": "true",
                    "properties": {
                        "ami-id": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "availability-zone": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "instance-id": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "instance-type": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "local-ipv4": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "public-hostname": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "public-ipv4": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "chunk": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "config": {
                    "dynamic": "true",
                    "properties": {
                        "deploymentId": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "runTaskAsCurrentUser": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "runTasksAsCurrentUser": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "framework": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "generic-worker": {
                    "dynamic": "true",
                    "properties": {
                        "go-arch": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "go-os": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "go-version": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "release": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "revision": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "source": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "version": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "key": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "machine": {
                    "dynamic": "true",
                    "properties": {
                        "aws": {"properties": {
                            "instance_type": {"properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}},
                            "~e~": {
                                "store": true,
                                "type": "long"
                            }
                        }},
                        "platform": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "tc_worker_type": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "machine-setup": {
                    "dynamic": "true",
                    "properties": {
                        "ami-created": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "created": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "maintainer": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "manifest": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "name": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "stats": {
                    "dynamic": "true",
                    "properties": {
                        "action": {
                            "dynamic": "true",
                            "properties": {
                                "assertion_count": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "crash": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "log": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "lsan_leak": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "lsan_summary": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "mozleak_object": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "mozleak_total": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "process_exit": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "process_output": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "process_start": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "test_status": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "bad_lines": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "bytes": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "duration": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "end_time": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "lines": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "ok": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "start_time": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "status": {
                            "dynamic": "true",
                            "properties": {
                                "crash": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "error": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "fail": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "none": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "ok": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "pass": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "skip": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "timeout": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "total": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "suite": {
                    "dynamic": "true",
                    "properties": {
                        "flavor": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "fullname": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "name": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "timestamp": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "type": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "task": {
            "dynamic": "true",
            "properties": {
                "capabilities": {
                    "dynamic": "true",
                    "properties": {
                        "devices": {
                            "dynamic": "true",
                            "properties": {
                                "loopbackVideo": {
                                    "dynamic": "true",
                                    "properties": {"~b~": {
                                        "store": true,
                                        "type": "boolean"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "privileged": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "deadline": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "dependencies": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "group": {"properties": {
                    "id": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "id": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "kind": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "maxRunTime": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "parent": {
                    "dynamic": "true",
                    "properties": {
                        "id": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "priority": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "provisioner": {"properties": {
                    "id": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "requires": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "scheduler": {"properties": {
                    "id": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "state": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "version": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "worker": {"properties": {
                    "group": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "id": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "type": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "treeherder": {
            "dynamic": "true",
            "properties": {
                "collection": {
                    "dynamic": "true",
                    "properties": {
                        "7,0/x86_64/debug": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "7,0/x86_64/opt": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "7-0/x86_64/debug": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "7-0/x86_64/opt": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "asan": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "debug": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "opt": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "pgo": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "groupName": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "groupSymbol": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "jobKind": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "labels": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "machine": {
                    "dynamic": "true",
                    "properties": {
                        "platform": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "symbol": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "tier": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "worker": {"properties": {"~e~": {
            "store": true,
            "type": "long"
        }}},
        "~e~": {
            "store": true,
            "type": "long"
        }
    }
## fx-test
    {
        "etl": {"properties": {
            "id": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "machine": {"properties": {
                "name": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "os": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "pid": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "python": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }},
            "source": {"properties": {
                "id": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "source": {"properties": {
                    "id": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "type": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }},
            "timestamp": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "type": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "~e~": {
                "store": true,
                "type": "long"
            }
        }},
        "result": {"properties": {
            "duration": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "end_time": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "expected": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "ok": {"properties": {"~b~": {
                "store": true,
                "type": "boolean"
            }}},
            "result": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "start_time": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "status": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "test": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "~e~": {
                "store": true,
                "type": "long"
            }
        }},
        "run": {
            "dynamic": "true",
            "properties": {
                "base url": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "build_id": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "build_number": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "build_tag": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "build_url": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "capabilities": {
                    "dynamic": "true",
                    "properties": {
                        "browsername": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "build": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "platform": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "public": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "version": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "driver": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "executor_number": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "git_branch": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "git_commit": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "git_url": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "jenkins_url": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "job_name": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "node_name": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "packages": {"properties": {
                    "~N~": {
                        "properties": {
                            "name": {"properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}},
                            "version": {"properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}},
                            "~e~": {
                                "store": true,
                                "type": "long"
                            }
                        },
                        "type": "nested"
                    },
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "platform": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "plugins": {"properties": {
                    "~N~": {
                        "properties": {
                            "name": {"properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}},
                            "version": {"properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}},
                            "~e~": {
                                "store": true,
                                "type": "long"
                            }
                        },
                        "type": "nested"
                    },
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "python": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "stats": {
                    "dynamic": "true",
                    "properties": {
                        "bytes": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "end_time": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "lines": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "ok": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "start_time": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "status": {
                            "dynamic": "true",
                            "properties": {
                                "error": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "fail": {"properties": {"~n~": {
                                    "store": true,
                                    "type": "double"
                                }}},
                                "pass": {"properties": {"~n~": {
                                    "store": true,
                                    "type": "double"
                                }}},
                                "skip": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "total": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "workspace": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "test": {"properties": {
            "file": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "full_name": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "name": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "option": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "suite": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "~e~": {
                "store": true,
                "type": "long"
            }
        }},
        "~e~": {
            "store": true,
            "type": "long"
        }
    }
## branches
    {
        "description": {
            "store": true,
            "type": "keyword"
        },
        "etl": {"properties": {"timestamp": {"type": "float"}}},
        "last_used": {"type": "long"},
        "locale": {
            "store": true,
            "type": "keyword"
        },
        "name": {
            "store": true,
            "type": "keyword"
        },
        "parent_name": {
            "store": true,
            "type": "keyword"
        },
        "url": {
            "store": true,
            "type": "keyword"
        }
    }
## repo
    {
        "bookmarks": {
            "store": true,
            "type": "keyword"
        },
        "branch": {"properties": {
            "description": {
                "store": true,
                "type": "keyword"
            },
            "etl": {"properties": {"timestamp": {
                "store": true,
                "type": "double"
            }}},
            "id": {
                "store": true,
                "type": "long"
            },
            "last_used": {
                "store": true,
                "type": "long"
            },
            "locale": {
                "store": true,
                "type": "keyword"
            },
            "name": {
                "store": true,
                "type": "keyword"
            },
            "parent_name": {
                "store": true,
                "type": "keyword"
            },
            "url": {
                "store": true,
                "type": "keyword"
            }
        }},
        "changeset": {"properties": {
            "author": {
                "store": true,
                "type": "keyword"
            },
            "backedoutby": {
                "store": true,
                "type": "keyword"
            },
            "bug": {
                "store": true,
                "type": "long"
            },
            "date": {
                "store": true,
                "type": "double"
            },
            "description": {
                "fields": {"words": {"type": "text"}},
                "store": true,
                "type": "keyword"
            },
            "diff": {
                "dynamic": "true",
                "properties": {"changes": {
                    "dynamic": "true",
                    "properties": {
                        "new": {
                            "dynamic": "true",
                            "properties": {"content": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "old": {
                            "dynamic": "true",
                            "properties": {"content": {
                                "store": true,
                                "type": "keyword"
                            }}
                        }
                    },
                    "type": "nested"
                }},
                "type": "nested"
            },
            "files": {
                "store": true,
                "type": "keyword"
            },
            "id": {
                "store": true,
                "type": "keyword"
            },
            "id12": {
                "store": true,
                "type": "keyword"
            },
            "moves": {
                "dynamic": "true",
                "properties": {
                    "changes": {
                        "dynamic": "true",
                        "properties": {
                            "action": {
                                "store": true,
                                "type": "keyword"
                            },
                            "line": {
                                "store": true,
                                "type": "long"
                            }
                        },
                        "type": "nested"
                    },
                    "new": {"properties": {"name": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "old": {"properties": {"name": {
                        "store": true,
                        "type": "keyword"
                    }}}
                },
                "type": "nested"
            }
        }},
        "children": {
            "store": true,
            "type": "keyword"
        },
        "error": {
            "store": true,
            "type": "keyword"
        },
        "etl": {"properties": {
            "machine": {"properties": {
                "name": {
                    "store": true,
                    "type": "keyword"
                },
                "os": {
                    "store": true,
                    "type": "keyword"
                },
                "pid": {
                    "store": true,
                    "type": "long"
                },
                "python": {
                    "store": true,
                    "type": "keyword"
                }
            }},
            "timestamp": {
                "store": true,
                "type": "double"
            }
        }},
        "index": {
            "store": true,
            "type": "long"
        },
        "landingsystem": {
            "store": true,
            "type": "keyword"
        },
        "parents": {
            "store": true,
            "type": "keyword"
        },
        "phase": {
            "store": true,
            "type": "keyword"
        },
        "push": {"properties": {
            "date": {
                "store": true,
                "type": "long"
            },
            "id": {
                "store": true,
                "type": "long"
            },
            "user": {
                "store": true,
                "type": "keyword"
            }
        }},
        "rev": {
            "store": true,
            "type": "long"
        },
        "tags": {
            "store": true,
            "type": "keyword"
        }
    }
## meta.columns
    null
## coverage
    {
        "action": {
            "dynamic": "true",
            "properties": {
                "duration": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "end_time": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "start_time": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "type": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "build": {
            "dynamic": "true",
            "properties": {
                "branch": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "date": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "platform": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "product": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "revision": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "revision12": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "task": {"properties": {"~e~": {
                    "store": true,
                    "type": "long"
                }}},
                "type": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "url": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "etl": {"properties": {
            "id": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "machine": {"properties": {
                "name": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "os": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "pid": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "python": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }},
            "source": {"properties": {
                "id": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "revision": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "source": {"properties": {
                    "id": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "machine": {"properties": {
                        "name": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "os": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "pid": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "python": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }},
                    "source": {"properties": {
                        "bucket": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "id": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "name": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "source": {"properties": {
                            "count": {"properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}},
                            "id": {"properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}},
                            "source": {"properties": {
                                "id": {"properties": {"~s~": {
                                    "store": true,
                                    "type": "keyword"
                                }}},
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }},
                            "type": {"properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}},
                            "~e~": {
                                "store": true,
                                "type": "long"
                            }
                        }},
                        "timestamp": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "type": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }},
                    "timestamp": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "type": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "timestamp": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "type": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "url": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }},
            "timestamp": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "type": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "~e~": {
                "store": true,
                "type": "long"
            }
        }},
        "repo": {
            "dynamic": "true",
            "properties": {
                "branch": {"properties": {
                    "locale": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "name": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "url": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "changeset": {
                    "dynamic": "true",
                    "properties": {
                        "author": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "backedoutby": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "bug": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "date": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "description": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "id": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "id12": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "index": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "landingsystem": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "push": {"properties": {
                    "date": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "id": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "user": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "rev": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "run": {
            "dynamic": "true",
            "properties": {
                "aws": {
                    "dynamic": "true",
                    "properties": {
                        "ami-id": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "availability-zone": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "instance-id": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "instance-type": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "local-ipv4": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "public-hostname": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "public-ipv4": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "chunk": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "config": {
                    "dynamic": "true",
                    "properties": {
                        "deploymentId": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "runTaskAsCurrentUser": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "runTasksAsCurrentUser": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "framework": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "generic-worker": {
                    "dynamic": "true",
                    "properties": {
                        "go-arch": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "go-os": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "go-version": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "release": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "revision": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "source": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "version": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "key": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "machine": {"properties": {
                    "aws": {"properties": {
                        "instance_type": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }},
                    "platform": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "tc_worker_type": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "machine-setup": {
                    "dynamic": "true",
                    "properties": {
                        "ami-created": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "created": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "maintainer": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "manifest": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "name": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "suite": {
                    "dynamic": "true",
                    "properties": {
                        "flavor": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "fullname": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "name": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "timestamp": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "type": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "source": {
            "dynamic": "true",
            "properties": {
                "file": {
                    "dynamic": "true",
                    "properties": {
                        "covered": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "is_firefox": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "name": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "old_name": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "percentage_covered": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "total_covered": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "total_uncovered": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "tuid_covered": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "tuid_uncovered": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "uncovered": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "is_file": {"properties": {"~b~": {
                    "store": true,
                    "type": "boolean"
                }}},
                "language": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "task": {"properties": {
            "capabilities": {"properties": {
                "devices": {"properties": {
                    "loopbackVideo": {"properties": {"~b~": {
                        "store": true,
                        "type": "boolean"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }},
            "deadline": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "dependencies": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "group": {"properties": {
                "id": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }},
            "id": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "kind": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "maxRunTime": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "parent": {"properties": {
                "id": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }},
            "priority": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "provisioner": {"properties": {
                "id": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }},
            "requires": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "scheduler": {"properties": {
                "id": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }},
            "state": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "version": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "worker": {"properties": {
                "group": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "id": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "type": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }},
            "~e~": {
                "store": true,
                "type": "long"
            }
        }},
        "test": {
            "dynamic": "true",
            "properties": {
                "chunk": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "name": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "suite": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "treeherder": {
            "dynamic": "true",
            "properties": {
                "collection": {
                    "dynamic": "true",
                    "properties": {
                        "ccov": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "debug": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "opt": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "groupName": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "groupSymbol": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "jobKind": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "machine": {"properties": {
                    "platform": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "symbol": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "tier": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "worker": {"properties": {"~e~": {
            "store": true,
            "type": "long"
        }}},
        "~e~": {
            "store": true,
            "type": "long"
        }
    }
## debug_tuid_service
    null
## jobs
    {
        "action": {
            "dynamic": "true",
            "properties": {
                "buildbot_status": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "builder": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "builder_time_zone": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "buildid": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "builduid": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "duration": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "end_time": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "etl": {"properties": {
                    "total_bytes": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "job_number": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "mozconfig_load_error": {
                    "dynamic": "true",
                    "properties": {"~b~": {
                        "store": true,
                        "type": "boolean"
                    }}
                },
                "reason": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "repack": {"properties": {"~b~": {
                    "store": true,
                    "type": "boolean"
                }}},
                "request_time": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "requests": {"properties": {
                    "request_id": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "timestamp": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "~N~": {
                        "properties": {
                            "request_id": {"properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}},
                            "timestamp": {"properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}},
                            "~e~": {
                                "store": true,
                                "type": "long"
                            }
                        },
                        "type": "nested"
                    },
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "start_time": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "step": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "timings": {"properties": {
                    "~N~": {
                        "properties": {
                            "builder": {"properties": {
                                "duration": {"properties": {"~n~": {
                                    "store": true,
                                    "type": "double"
                                }}},
                                "elapsedtime": {"properties": {"~n~": {
                                    "store": true,
                                    "type": "double"
                                }}},
                                "end_time": {"properties": {"~n~": {
                                    "store": true,
                                    "type": "double"
                                }}},
                                "parts": {"properties": {"~s~": {
                                    "store": true,
                                    "type": "keyword"
                                }}},
                                "raw_step": {"properties": {"~s~": {
                                    "store": true,
                                    "type": "keyword"
                                }}},
                                "start_time": {"properties": {"~n~": {
                                    "store": true,
                                    "type": "double"
                                }}},
                                "status": {"properties": {"~s~": {
                                    "store": true,
                                    "type": "keyword"
                                }}},
                                "step": {"properties": {"~s~": {
                                    "store": true,
                                    "type": "keyword"
                                }}},
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }},
                            "harness": {"properties": {
                                "duration": {"properties": {"~n~": {
                                    "store": true,
                                    "type": "double"
                                }}},
                                "end_time": {"properties": {"~n~": {
                                    "store": true,
                                    "type": "double"
                                }}},
                                "mode": {"properties": {"~s~": {
                                    "store": true,
                                    "type": "keyword"
                                }}},
                                "result": {"properties": {"~s~": {
                                    "store": true,
                                    "type": "keyword"
                                }}},
                                "start_time": {"properties": {"~n~": {
                                    "store": true,
                                    "type": "double"
                                }}},
                                "step": {"properties": {"~s~": {
                                    "store": true,
                                    "type": "keyword"
                                }}},
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }},
                            "order": {"properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}},
                            "~e~": {
                                "store": true,
                                "type": "long"
                            }
                        },
                        "type": "nested"
                    },
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "type": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "build": {"properties": {
            "branch": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "date": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "id": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "locale": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "locales": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "name": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "platform": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "product": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "release": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "revision": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "revision12": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "trigger": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "type": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "url": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "version": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "~e~": {
                "store": true,
                "type": "long"
            }
        }},
        "etl": {"properties": {
            "file": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "id": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "revision": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "source": {"properties": {
                "id": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "source": {"properties": {
                    "id": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "timestamp": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "type": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "url": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "timestamp": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "type": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }},
            "timestamp": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "type": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "~e~": {
                "store": true,
                "type": "long"
            }
        }},
        "other": {"properties": {
            "name": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "value": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "~N~": {
                "properties": {
                    "name": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "value": {"properties": {
                        "~b~": {
                            "store": true,
                            "type": "boolean"
                        },
                        "~n~": {
                            "store": true,
                            "type": "double"
                        },
                        "~s~": {
                            "store": true,
                            "type": "keyword"
                        }
                    }},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                },
                "type": "nested"
            },
            "~e~": {
                "store": true,
                "type": "long"
            }
        }},
        "properties": {"properties": {
            "appname": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "appversion": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "aws_ami_id": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "basedir": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "builddir": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "builduid": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "comments": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "got_revision": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "master": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "packagefilename": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "scheduler": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "slavebuilddir": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "sourcestamp": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "stage_platform": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "symbolsurl": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "testpackagesurl": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "testsurl": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "uploadfiles": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "~e~": {
                "store": true,
                "type": "long"
            }
        }},
        "repo": {
            "dynamic": "true",
            "properties": {
                "branch": {"properties": {
                    "locale": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "name": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "url": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "changeset": {
                    "dynamic": "true",
                    "properties": {
                        "author": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "backedoutby": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "bug": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "date": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "description": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "id": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "id12": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "index": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "push": {"properties": {
                    "date": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "id": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "user": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "rev": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "tags": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "run": {"properties": {
            "chunk": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "files": {"properties": {
                "~N~": {
                    "properties": {
                        "name": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "url": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    },
                    "type": "nested"
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }},
            "key": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "logurl": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "machine": {"properties": {
                "aws_id": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "aws_type": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "name": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "os": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "pool": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "type": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }},
            "script": {"properties": {
                "revision": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }},
            "suite": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "type": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "~e~": {
                "store": true,
                "type": "long"
            }
        }},
        "tags": {"properties": {"~s~": {
            "store": true,
            "type": "keyword"
        }}},
        "task": {"properties": {
            "id": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "~e~": {
                "store": true,
                "type": "long"
            }
        }},
        "~e~": {
            "store": true,
            "type": "long"
        }
    }
## debug-etl
    null
## treeherder
    {
        "action": {"properties": {
            "duration": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "end_time": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "request_time": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "start_time": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "~e~": {
                "store": true,
                "type": "long"
            }
        }},
        "bugs": {
            "dynamic": "true",
            "properties": {
                "bug_id": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "created": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "~N~": {
                    "dynamic": "true",
                    "properties": {
                        "bug_id": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "created": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    },
                    "type": "nested"
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "build": {"properties": {
            "architecture": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "branch": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "date": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "os": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "platform": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "product": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "revision": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "revision12": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "type": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "~e~": {
                "store": true,
                "type": "long"
            }
        }},
        "etl": {"properties": {
            "id": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "machine": {"properties": {
                "name": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "os": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "pid": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "python": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }},
            "revision": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "source": {"properties": {
                "id": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "machine": {"properties": {
                    "name": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "os": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "pid": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "python": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "revision": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "source": {"properties": {
                    "id": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "type": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }},
            "timestamp": {"properties": {"~n~": {
                "store": true,
                "type": "double"
            }}},
            "type": {"properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}},
            "~e~": {
                "store": true,
                "type": "long"
            }
        }},
        "failure": {
            "dynamic": "true",
            "properties": {
                "auto_classification": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "classification": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "notes": {
                    "dynamic": "true",
                    "properties": {
                        "created": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "failure_classification": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "text": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~N~": {
                            "dynamic": "true",
                            "properties": {
                                "created": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "failure_classification": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "text": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            },
                            "type": "nested"
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "job": {
            "dynamic": "true",
            "properties": {
                "details": {
                    "dynamic": "true",
                    "properties": {
                        "title": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "url": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "value": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~N~": {
                            "properties": {
                                "title": {"properties": {"~s~": {
                                    "store": true,
                                    "type": "keyword"
                                }}},
                                "url": {"properties": {"~s~": {
                                    "store": true,
                                    "type": "keyword"
                                }}},
                                "value": {"properties": {"~s~": {
                                    "store": true,
                                    "type": "keyword"
                                }}},
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            },
                            "type": "nested"
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "guid": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "id": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "type": {
                    "dynamic": "true",
                    "properties": {
                        "description": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "group": {"properties": {
                            "name": {"properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}},
                            "symbol": {"properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}},
                            "~e~": {
                                "store": true,
                                "type": "long"
                            }
                        }},
                        "name": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "symbol": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "job_log": {
            "dynamic": "true",
            "properties": {
                "name": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "status": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "url": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~N~": {
                    "dynamic": "true",
                    "properties": {
                        "failure_line": {
                            "dynamic": "true",
                            "properties": {
                                "action": {"properties": {"~s~": {
                                    "store": true,
                                    "type": "keyword"
                                }}},
                                "best_is_verified": {"properties": {"~n~": {
                                    "store": true,
                                    "type": "double"
                                }}},
                                "created": {"properties": {"~n~": {
                                    "store": true,
                                    "type": "double"
                                }}},
                                "line": {"properties": {"~n~": {
                                    "store": true,
                                    "type": "double"
                                }}},
                                "modified": {"properties": {"~n~": {
                                    "store": true,
                                    "type": "double"
                                }}},
                                "repository": {"properties": {"~s~": {
                                    "store": true,
                                    "type": "keyword"
                                }}},
                                "~N~": {
                                    "dynamic": "true",
                                    "properties": {
                                        "action": {"properties": {"~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }}},
                                        "best_classification": {
                                            "dynamic": "true",
                                            "properties": {
                                                "bug_number": {
                                                    "dynamic": "true",
                                                    "properties": {"~n~": {
                                                        "store": true,
                                                        "type": "double"
                                                    }}
                                                },
                                                "created": {
                                                    "dynamic": "true",
                                                    "properties": {"~n~": {
                                                        "store": true,
                                                        "type": "double"
                                                    }}
                                                },
                                                "modified": {
                                                    "dynamic": "true",
                                                    "properties": {"~n~": {
                                                        "store": true,
                                                        "type": "double"
                                                    }}
                                                },
                                                "~e~": {
                                                    "store": true,
                                                    "type": "long"
                                                }
                                            }
                                        },
                                        "best_is_verified": {"properties": {"~n~": {
                                            "store": true,
                                            "type": "double"
                                        }}},
                                        "created": {"properties": {"~n~": {
                                            "store": true,
                                            "type": "double"
                                        }}},
                                        "expected": {"properties": {"~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }}},
                                        "failure_match": {
                                            "dynamic": "true",
                                            "properties": {
                                                "classified_failure": {
                                                    "dynamic": "true",
                                                    "properties": {
                                                        "bug_number": {
                                                            "dynamic": "true",
                                                            "properties": {"~n~": {
                                                                "store": true,
                                                                "type": "double"
                                                            }}
                                                        },
                                                        "created": {
                                                            "dynamic": "true",
                                                            "properties": {"~n~": {
                                                                "store": true,
                                                                "type": "double"
                                                            }}
                                                        },
                                                        "modified": {
                                                            "dynamic": "true",
                                                            "properties": {"~n~": {
                                                                "store": true,
                                                                "type": "double"
                                                            }}
                                                        },
                                                        "~e~": {
                                                            "store": true,
                                                            "type": "long"
                                                        }
                                                    }
                                                },
                                                "matcher": {
                                                    "dynamic": "true",
                                                    "properties": {"~s~": {
                                                        "store": true,
                                                        "type": "keyword"
                                                    }}
                                                },
                                                "score": {
                                                    "dynamic": "true",
                                                    "properties": {"~n~": {
                                                        "store": true,
                                                        "type": "double"
                                                    }}
                                                },
                                                "~N~": {
                                                    "dynamic": "true",
                                                    "properties": {
                                                        "classified_failure": {
                                                            "dynamic": "true",
                                                            "properties": {
                                                                "bug_number": {
                                                                    "dynamic": "true",
                                                                    "properties": {"~n~": {
                                                                        "store": true,
                                                                        "type": "double"
                                                                    }}
                                                                },
                                                                "created": {
                                                                    "dynamic": "true",
                                                                    "properties": {"~n~": {
                                                                        "store": true,
                                                                        "type": "double"
                                                                    }}
                                                                },
                                                                "modified": {
                                                                    "dynamic": "true",
                                                                    "properties": {"~n~": {
                                                                        "store": true,
                                                                        "type": "double"
                                                                    }}
                                                                },
                                                                "~e~": {
                                                                    "store": true,
                                                                    "type": "long"
                                                                }
                                                            }
                                                        },
                                                        "matcher": {
                                                            "dynamic": "true",
                                                            "properties": {"~s~": {
                                                                "store": true,
                                                                "type": "keyword"
                                                            }}
                                                        },
                                                        "matcher_name": {
                                                            "dynamic": "true",
                                                            "properties": {"~s~": {
                                                                "store": true,
                                                                "type": "keyword"
                                                            }}
                                                        },
                                                        "score": {
                                                            "dynamic": "true",
                                                            "properties": {"~n~": {
                                                                "store": true,
                                                                "type": "double"
                                                            }}
                                                        },
                                                        "~e~": {
                                                            "store": true,
                                                            "type": "long"
                                                        }
                                                    },
                                                    "type": "nested"
                                                },
                                                "~e~": {
                                                    "store": true,
                                                    "type": "long"
                                                }
                                            }
                                        },
                                        "level": {"properties": {"~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }}},
                                        "line": {"properties": {"~n~": {
                                            "store": true,
                                            "type": "double"
                                        }}},
                                        "modified": {"properties": {"~n~": {
                                            "store": true,
                                            "type": "double"
                                        }}},
                                        "repository": {"properties": {"~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }}},
                                        "signature": {"properties": {"~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }}},
                                        "stack": {"properties": {"~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }}},
                                        "status": {"properties": {"~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }}},
                                        "test": {"properties": {"~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }}},
                                        "~e~": {
                                            "store": true,
                                            "type": "long"
                                        }
                                    },
                                    "type": "nested"
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "name": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "status": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "url": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    },
                    "type": "nested"
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "last_modified": {"properties": {"~n~": {
            "store": true,
            "type": "double"
        }}},
        "repo": {
            "dynamic": "true",
            "properties": {
                "branch": {"properties": {
                    "locale": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "name": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "url": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "changeset": {
                    "dynamic": "true",
                    "properties": {
                        "author": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "backedoutby": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "bug": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "date": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "description": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "id": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "id12": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "error": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "index": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "landingsystem": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "push": {"properties": {
                    "date": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "id": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "user": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "rev": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "tags": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "run": {
            "dynamic": "true",
            "properties": {
                "buildbot": {
                    "dynamic": "true",
                    "properties": {
                        "id": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "key": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "machine": {"properties": {
                    "architecture": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "name": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "os": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "platform": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "pool": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "reason": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "result": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "state": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "taskcluster": {"properties": {
                    "id": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "retry_id": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "tier": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "stats": {
            "dynamic": "true",
            "properties": {
                "cpu_idle": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "cpu_io_wait": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "cpu_system": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "cpu_usage": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "cpu_user": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "io_read_bytes": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "io_read_time": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "io_write_bytes": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "io_write_time": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "linker_max_vsize": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "swap_in": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "swap_out": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "~e~": {
            "store": true,
            "type": "long"
        }
    }
## debug-spot-manager
    null
## perf
    {
        "action": {
            "dynamic": "true",
            "properties": {
                "buildbot_status": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "duration": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "end_time": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "job_number": {
                    "dynamic": "true",
                    "properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}
                },
                "repack": {
                    "dynamic": "true",
                    "properties": {"~b~": {
                        "store": true,
                        "type": "boolean"
                    }}
                },
                "start_time": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "step": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "type": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "alertChangeType": {"properties": {"~s~": {
            "store": true,
            "type": "keyword"
        }}},
        "alertThreshold": {"properties": {"~n~": {
            "store": true,
            "type": "double"
        }}},
        "build": {
            "dynamic": "true",
            "properties": {
                "action": {
                    "dynamic": "true",
                    "properties": {
                        "duration": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "end_time": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "start_time": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "type": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "branch": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "channel": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "compiler": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "date": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "id": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "locale": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "locales": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "name": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "platform": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "product": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "release": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "repo": {
                    "dynamic": "true",
                    "properties": {
                        "branch": {
                            "dynamic": "true",
                            "properties": {
                                "locale": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "name": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "url": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "changeset": {
                            "dynamic": "true",
                            "properties": {
                                "author": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "backedoutby": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "bug": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "date": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "description": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "id": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "id12": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "index": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "push": {
                            "dynamic": "true",
                            "properties": {
                                "date": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "id": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "user": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "rev": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "tags": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "revision": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "revision12": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "run": {
                    "dynamic": "true",
                    "properties": {
                        "key": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "machine": {
                            "dynamic": "true",
                            "properties": {
                                "aws": {
                                    "dynamic": "true",
                                    "properties": {
                                        "instance_type": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "~e~": {
                                            "store": true,
                                            "type": "long"
                                        }
                                    }
                                },
                                "platform": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "tc_worker_type": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "name": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "suite": {
                            "dynamic": "true",
                            "properties": {"~e~": {
                                "store": true,
                                "type": "long"
                            }}
                        },
                        "timestamp": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "tags": {
                    "dynamic": "true",
                    "type": "object"
                },
                "task": {
                    "dynamic": "true",
                    "properties": {
                        "id": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "train": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "treeherder": {
                    "dynamic": "true",
                    "properties": {
                        "build": {
                            "dynamic": "true",
                            "properties": {
                                "platform": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "collection": {
                            "dynamic": "true",
                            "properties": {
                                "asan": {
                                    "dynamic": "true",
                                    "properties": {"~b~": {
                                        "store": true,
                                        "type": "boolean"
                                    }}
                                },
                                "ccov": {
                                    "dynamic": "true",
                                    "properties": {"~b~": {
                                        "store": true,
                                        "type": "boolean"
                                    }}
                                },
                                "debug": {
                                    "dynamic": "true",
                                    "properties": {"~b~": {
                                        "store": true,
                                        "type": "boolean"
                                    }}
                                },
                                "fips": {
                                    "dynamic": "true",
                                    "properties": {"~b~": {
                                        "store": true,
                                        "type": "boolean"
                                    }}
                                },
                                "fuzz": {
                                    "dynamic": "true",
                                    "properties": {"~b~": {
                                        "store": true,
                                        "type": "boolean"
                                    }}
                                },
                                "make": {
                                    "dynamic": "true",
                                    "properties": {"~b~": {
                                        "store": true,
                                        "type": "boolean"
                                    }}
                                },
                                "opt": {
                                    "dynamic": "true",
                                    "properties": {"~b~": {
                                        "store": true,
                                        "type": "boolean"
                                    }}
                                },
                                "pgo": {
                                    "dynamic": "true",
                                    "properties": {"~b~": {
                                        "store": true,
                                        "type": "boolean"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "groupName": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "groupSymbol": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "jobKind": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "machine": {
                            "dynamic": "true",
                            "properties": {
                                "platform": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "symbol": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "tier": {
                            "dynamic": "true",
                            "properties": {"~n~": {
                                "store": true,
                                "type": "double"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "trigger": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "type": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "url": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "version": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "worker": {
                    "dynamic": "true",
                    "properties": {"~e~": {
                        "store": true,
                        "type": "long"
                    }}
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "etl": {
            "dynamic": "true",
            "properties": {
                "id": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "revision": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "source": {
                    "dynamic": "true",
                    "properties": {
                        "error": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "id": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "name": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "revision": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "source": {
                            "dynamic": "true",
                            "properties": {
                                "duration": {
                                    "dynamic": "true",
                                    "properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}
                                },
                                "file": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "id": {"properties": {"~n~": {
                                    "store": true,
                                    "type": "double"
                                }}},
                                "machine": {"properties": {
                                    "name": {"properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}},
                                    "os": {"properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}},
                                    "pid": {"properties": {"~n~": {
                                        "store": true,
                                        "type": "double"
                                    }}},
                                    "python": {"properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}},
                                    "~e~": {
                                        "store": true,
                                        "type": "long"
                                    }
                                }},
                                "name": {"properties": {"~s~": {
                                    "store": true,
                                    "type": "keyword"
                                }}},
                                "revision": {"properties": {"~s~": {
                                    "store": true,
                                    "type": "keyword"
                                }}},
                                "source": {
                                    "dynamic": "true",
                                    "properties": {
                                        "bucket": {"properties": {"~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }}},
                                        "error": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "id": {"properties": {"~n~": {
                                            "store": true,
                                            "type": "double"
                                        }}},
                                        "machine": {"properties": {
                                            "name": {"properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}},
                                            "os": {"properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}},
                                            "pid": {"properties": {"~n~": {
                                                "store": true,
                                                "type": "double"
                                            }}},
                                            "python": {"properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}},
                                            "~e~": {
                                                "store": true,
                                                "type": "long"
                                            }
                                        }},
                                        "name": {"properties": {"~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }}},
                                        "source": {
                                            "dynamic": "true",
                                            "properties": {
                                                "bucket": {"properties": {"~s~": {
                                                    "store": true,
                                                    "type": "keyword"
                                                }}},
                                                "count": {"properties": {"~n~": {
                                                    "store": true,
                                                    "type": "double"
                                                }}},
                                                "exchange": {
                                                    "dynamic": "true",
                                                    "properties": {"~s~": {
                                                        "store": true,
                                                        "type": "keyword"
                                                    }}
                                                },
                                                "id": {"properties": {"~n~": {
                                                    "store": true,
                                                    "type": "double"
                                                }}},
                                                "name": {"properties": {"~s~": {
                                                    "store": true,
                                                    "type": "keyword"
                                                }}},
                                                "sent": {
                                                    "dynamic": "true",
                                                    "properties": {"~n~": {
                                                        "store": true,
                                                        "type": "double"
                                                    }}
                                                },
                                                "source": {"properties": {
                                                    "count": {"properties": {"~n~": {
                                                        "store": true,
                                                        "type": "double"
                                                    }}},
                                                    "id": {"properties": {
                                                        "~n~": {
                                                            "store": true,
                                                            "type": "double"
                                                        },
                                                        "~s~": {
                                                            "store": true,
                                                            "type": "keyword"
                                                        }
                                                    }},
                                                    "source": {"properties": {
                                                        "id": {"properties": {"~s~": {
                                                            "store": true,
                                                            "type": "keyword"
                                                        }}},
                                                        "~e~": {
                                                            "store": true,
                                                            "type": "long"
                                                        }
                                                    }},
                                                    "type": {"properties": {"~s~": {
                                                        "store": true,
                                                        "type": "keyword"
                                                    }}},
                                                    "~e~": {
                                                        "store": true,
                                                        "type": "long"
                                                    }
                                                }},
                                                "timestamp": {"properties": {"~n~": {
                                                    "store": true,
                                                    "type": "double"
                                                }}},
                                                "type": {"properties": {"~s~": {
                                                    "store": true,
                                                    "type": "keyword"
                                                }}},
                                                "~e~": {
                                                    "store": true,
                                                    "type": "long"
                                                }
                                            }
                                        },
                                        "timestamp": {"properties": {"~n~": {
                                            "store": true,
                                            "type": "double"
                                        }}},
                                        "type": {"properties": {"~s~": {
                                            "store": true,
                                            "type": "keyword"
                                        }}},
                                        "~e~": {
                                            "store": true,
                                            "type": "long"
                                        }
                                    }
                                },
                                "timestamp": {"properties": {"~n~": {
                                    "store": true,
                                    "type": "double"
                                }}},
                                "type": {"properties": {"~s~": {
                                    "store": true,
                                    "type": "keyword"
                                }}},
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            }
                        },
                        "timestamp": {"properties": {"~n~": {
                            "store": true,
                            "type": "double"
                        }}},
                        "type": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "url": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "timestamp": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "type": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "other": {
            "dynamic": "true",
            "properties": {
                "~N~": {
                    "dynamic": "true",
                    "properties": {
                        "name": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "value": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    },
                    "type": "nested"
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "properties": {
            "dynamic": "true",
            "properties": {"~e~": {
                "store": true,
                "type": "long"
            }}
        },
        "repo": {
            "dynamic": "true",
            "properties": {
                "branch": {"properties": {
                    "locale": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "name": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "url": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "changeset": {"properties": {
                    "author": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "backedoutby": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "bug": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "date": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "description": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "id": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "id12": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "error": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "index": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "landingsystem": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "push": {"properties": {
                    "date": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "id": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "user": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "rev": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "tags": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "result": {
            "dynamic": "true",
            "properties": {
                "control_replicates": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "extraOptions": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "framework": {"properties": {
                    "name": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "lower_is_better": {"properties": {"~b~": {
                    "store": true,
                    "type": "boolean"
                }}},
                "ordering": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "raw_replicates": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "rejects": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "samples": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "stats": {"properties": {
                    "count": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "first": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "kurtosis": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "last": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "max": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "mean": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "median": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "min": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "s0": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "s1": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "s2": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "s3": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "s4": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "skew": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "std": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "variance": {"properties": {"~n~": {
                        "store": true,
                        "type": "double"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "suite": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "test": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "unit": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "value": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "run": {
            "dynamic": "true",
            "properties": {
                "aws": {"properties": {
                    "ami-id": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "availability-zone": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "instance-id": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "instance-type": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "local-ipv4": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "public-hostname": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "public-ipv4": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "browser": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "chunk": {
                    "dynamic": "true",
                    "properties": {
                        "~n~": {
                            "store": true,
                            "type": "double"
                        },
                        "~s~": {
                            "store": true,
                            "type": "keyword"
                        }
                    }
                },
                "config": {
                    "dynamic": "true",
                    "properties": {
                        "deploymentId": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "runTaskAsCurrentUser": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "runTasksAsCurrentUser": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "files": {
                    "dynamic": "true",
                    "properties": {
                        "~N~": {
                            "dynamic": "true",
                            "properties": {
                                "name": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "url": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            },
                            "type": "nested"
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "framework": {"properties": {
                    "name": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    },
                    "~s~": {
                        "store": true,
                        "type": "keyword"
                    }
                }},
                "generic-worker": {"properties": {
                    "go-arch": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "go-os": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "go-version": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "release": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "revision": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "source": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "version": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "key": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "logurl": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "machine": {
                    "dynamic": "true",
                    "properties": {
                        "architecture": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "aws": {"properties": {
                            "instance_type": {"properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}},
                            "~e~": {
                                "store": true,
                                "type": "long"
                            }
                        }},
                        "name": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "os": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "platform": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "platfrom": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "pool": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "tc_worker_type": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "type": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "machine-setup": {
                    "dynamic": "true",
                    "properties": {
                        "ami-created": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "created": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "info": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "maintainer": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "manifest": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "note": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "rollback": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "script": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "name": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "os": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "stats": {"properties": {"~e~": {
                    "store": true,
                    "type": "long"
                }}},
                "suite": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "timestamp": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "trigger": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "type": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "tags": {
            "dynamic": "true",
            "properties": {"~s~": {
                "store": true,
                "type": "keyword"
            }}
        },
        "task": {
            "dynamic": "true",
            "properties": {
                "capabilities": {"properties": {
                    "devices": {"properties": {
                        "loopbackAudio": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "loopbackVideo": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }},
                    "privileged": {"properties": {"~b~": {
                        "store": true,
                        "type": "boolean"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "deadline": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "dependencies": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "group": {"properties": {
                    "id": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "id": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "kind": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "maxRunTime": {
                    "dynamic": "true",
                    "properties": {
                        "~n~": {
                            "store": true,
                            "type": "double"
                        },
                        "~s~": {
                            "store": true,
                            "type": "keyword"
                        }
                    }
                },
                "mounts": {
                    "dynamic": "true",
                    "properties": {
                        "~N~": {
                            "dynamic": "true",
                            "properties": {
                                "cacheName": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "content": {
                                    "dynamic": "true",
                                    "properties": {
                                        "artifact": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "file": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "sha256": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "sha356": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "taskId": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "url": {
                                            "dynamic": "true",
                                            "properties": {"~s~": {
                                                "store": true,
                                                "type": "keyword"
                                            }}
                                        },
                                        "~e~": {
                                            "store": true,
                                            "type": "long"
                                        }
                                    }
                                },
                                "directory": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "file": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "format": {
                                    "dynamic": "true",
                                    "properties": {"~s~": {
                                        "store": true,
                                        "type": "keyword"
                                    }}
                                },
                                "~e~": {
                                    "store": true,
                                    "type": "long"
                                }
                            },
                            "type": "nested"
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "parent": {"properties": {
                    "id": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "priority": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "provisioner": {"properties": {
                    "id": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "requires": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "scheduler": {"properties": {
                    "id": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "state": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "version": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "worker": {"properties": {
                    "group": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "id": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "type": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "treeherder": {
            "dynamic": "true",
            "properties": {
                "build": {"properties": {
                    "platform": {"properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}},
                    "~e~": {
                        "store": true,
                        "type": "long"
                    }
                }},
                "collection": {
                    "dynamic": "true",
                    "properties": {
                        "32": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "64": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "7,0/x86_64/debug": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "7,0/x86_64/opt": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "7-0/x86_64/debug": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "7-0/x86_64/opt": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "86": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "all": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "asan": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "ccov": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "cfi": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "debug": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "fips": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "fuzz": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "lto": {
                            "dynamic": "true",
                            "properties": {"~b~": {
                                "store": true,
                                "type": "boolean"
                            }}
                        },
                        "make": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "opt": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "opt/opt": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "opt/opt/opt": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "opt/opt/opt/opt": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "opt/opt/opt/opt/opt": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "pgo": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "pgo/opt": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "pgo/opt/opt": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "release": {"properties": {"~b~": {
                            "store": true,
                            "type": "boolean"
                        }}},
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "groupName": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "groupSymbol": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "jobKind": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "jobSymbol": {
                    "dynamic": "true",
                    "properties": {"~s~": {
                        "store": true,
                        "type": "keyword"
                    }}
                },
                "labels": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "machine": {
                    "dynamic": "true",
                    "properties": {
                        "architecture": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "os": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "platform": {"properties": {"~s~": {
                            "store": true,
                            "type": "keyword"
                        }}},
                        "platfrom": {
                            "dynamic": "true",
                            "properties": {"~s~": {
                                "store": true,
                                "type": "keyword"
                            }}
                        },
                        "~e~": {
                            "store": true,
                            "type": "long"
                        }
                    }
                },
                "productName": {"properties": {"~s~": {
                    "store": true,
                    "type": "keyword"
                }}},
                "symbol": {"properties": {
                    "~n~": {
                        "store": true,
                        "type": "double"
                    },
                    "~s~": {
                        "store": true,
                        "type": "keyword"
                    }
                }},
                "tier": {"properties": {"~n~": {
                    "store": true,
                    "type": "double"
                }}},
                "~e~": {
                    "store": true,
                    "type": "long"
                }
            }
        },
        "type": {"properties": {"~s~": {
            "store": true,
            "type": "keyword"
        }}},
        "unit": {"properties": {"~s~": {
            "store": true,
            "type": "keyword"
        }}},
        "units": {"properties": {"~s~": {
            "store": true,
            "type": "keyword"
        }}},
        "worker": {"properties": {"~e~": {
            "store": true,
            "type": "long"
        }}},
        "~e~": {
            "store": true,
            "type": "long"
        }
    }
