# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import division
from __future__ import unicode_literals

from mo_dots import Data


class Revision(Data):
    def __hash__(self):
        return hash((self.branch.name.lower(), self.changeset.id[:12]))

    def __eq__(self, other):
        if other == None:
            return False
        return (self.branch.name.lower(), self.changeset.id[:12]) == (other.branch.name.lower(), other.changeset.id[:12])


revision_schema = {


    "settings": {
        "index.number_of_replicas": 1,
        "index.number_of_shards": 6,
        "analysis": {
            "tokenizer": {
                "left250": {
                    "type": "pattern",
                    "pattern": "^.{1,250}"
                }
            },
            "analyzer": {
                "description_limit": {
                    "type": "custom",
                    "tokenizer": "left250",
                    "filter": [
                        "lowercase",
                        "asciifolding"
                    ]
                }
            }
        }
    },
    "mappings": {
        "revision": {
            "_all": {
                "enabled": False
            },
            "dynamic_templates": [
                {
                    "default_strings": {
                        "mapping": {
                            "type": "keyword"
                        },
                        "match_mapping_type": "string",
                        "match": "*"
                    }
                }
            ],
            "properties": {
                "changeset": {
                    "type": "object",
                    "properties": {
                        "description": {
                            "index": True,
                            "type": "text",
                            "fields": {
                                "raw": {
                                    "type": "text",
                                    "analyzer": "description_limit"
                                }
                            }
                        },
                        "diff": {
                            "type": "nested",
                            "dynamic": True,
                            "properties": {
                                "changes": {
                                    "type": "nested",
                                    "dynamic": True,
                                    "properties": {
                                        "new": {
                                            "type": "object",
                                            "dynamic": True,
                                            "properties": {
                                                "content": {
                                                    "type": "keyword"
                                                }
                                            }
                                        },
                                        "old": {
                                            "type": "object",
                                            "dynamic": True,
                                            "properties": {
                                                "content": {
                                                    "type": "keyword"
                                                }
                                            }
                                        }
                                    }
                                }

                            }
                        }
                    }
                }
            }
        }
    }
}
