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

DEBUG = True


def expect(*names):
    # GET MODULE OF THE CALLER
    caller_frame = inspect.stack()[1]
    caller = inspect.getmodule(caller_frame[0])

    # REGISTER DESIRED EXPORT
    output = []
    for name in names:
        desc = Expecting(caller, name, caller_frame)
        setattr(caller, name, desc)
        output.append(desc)
        if DEBUG:
            print(">>> " + desc.module.__name__ + " is expecting " + name)

    return output


class Expecting(object):
    __slots__ = ["module", "name", "frame", "timeout"]

    def __init__(self, module, name, frame):
        """
        PLACEHOLDER FOR A LATER VALUE
        :param module:
        :param name:
        :param frame:
        """
        _set = object.__setattr__

        _set(self, "module", module)
        _set(self, "name", name)
        _set(self, "frame", frame)
        timer = Timer(10.0, self, args=[])
        timer.start()
        _set(self, "timeout", timer)

    def __call__(self, *args, **kwargs):
        raise Exception(
            "missing expected call export(\"" + self.module.__name__ + "\", " + self.name + ")"
        )

    def __getattr__(self, item):
        if item in Expecting.__slots__:
            object.__getattribute__(self, item)
        self()

    def __setattr__(self, key, value):
        self()

    def __getitem__(self, item):
        self()


def export(module, name, value=None):
    if isinstance(module, text):
        module = importlib.import_module(module)
    if not isinstance(name, text):
        # GET MODULE OF THE CALLER TO FIND NAME OF OBJECT
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
            raise Exception(
                "Can not find variable holding a " + value.__class__.__name__
            )

    desc = getattr(module, name, None)
    if isinstance(desc, Expecting):
        desc.timeout.cancel()
        if DEBUG:
            print(">>> " + module.__name__ + " got expected " + name)
    else:
        raise Exception(module.__name__ + " is not expecting an export to " + name)

    setattr(module, name, value)
