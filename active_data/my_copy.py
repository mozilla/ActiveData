# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import

import os
from pyLibrary.env.files import File

for subdir, dirs, files in os.walk("c:/users/kyle/code"):
    for file in files:
        #print os.path.join(subdir, file)
        filepath = subdir + os.sep + file

        if ".idea" in filepath or filepath.endswith(".iml"):
            file = File(filepath)
            newfile = File(file.abspath.replace("c:/users/kyle/", "e:/"))
            newfile.write_bytes(file.read_bytes())
