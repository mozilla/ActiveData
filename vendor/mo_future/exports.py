# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

import importlib
import inspect
from threading import Timer

from mo_future import text

expected = {}  # MAP FROM (MODULE, VARIABLE) PAIR TO STATUS


def expect(*names):
    # GET MODULE OF THE CALLER
    caller_frame = inspect.stack()[1]
    caller = inspect.getmodule(caller_frame[0])

    # REGISTER DESIRED EXPORT
    for name in names:
        desc = {
            "module": caller,
            "name": name,
            "frame": caller_frame,
        }
        timeout = Timer(10.0, on_timeout, args=(desc,))
        desc["timeout"] = timeout

        expected[(caller.__name__, name)] = desc
        setattr(caller, name, None)

    return [None] * len(names)


def export(module, name, value=None):
    if isinstance(module, text):
        module = importlib.import_module(module)
    if not isinstance(name, text):
        # GET MODULE OF THE CALLER
        value = name
        frame = inspect.stack()[1]
        caller = inspect.getmodule(frame[0])
        for n in dir(caller):
            try:
                if getattr(caller, n) is value:
                    name = n
                    break
            except Exception:
                pass
        else:
            raise Exception("Can not find variable holding a " + value.__class__.__name__)

    setattr(module, name, value)
    # DEREGISTER
    desc = expected.get((module.__name__, name))
    if not desc:
        raise Exception(module + " is not expecting an export to " + name)
    desc["timeout"].cancel()
    del expected[(module.__name__, name)]


def on_timeout(desc):
    raise Exception(desc.name + " defined in " + desc.module + " has not been assigned")
