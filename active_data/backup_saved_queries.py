# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from pyLibrary import convert
from mo_logs import constants, startup
from mo_logs import Log
from pyLibrary.env import elasticsearch
from mo_files import File


def main():
    try:
        config = startup.read_settings(defs=[{
            "name": ["--file"],
            "help": "file to save backup",
            "type": str,
            "dest": "file",
            "required": True
        }])
        constants.set(config.constants)
        Log.start(config.debug)

        sq = elasticsearch.Index(kwargs=config.saved_queries)
        result = sq.search({
            "query": {
                "match_all": {}
            },
            "size": 200000
        })

        File(config.args.file).write("".join(map(convert.json2value, result.hits.hits)))

    except Exception, e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()

