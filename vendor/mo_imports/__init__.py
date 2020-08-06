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
import sys
from threading import Thread
from time import time, sleep

from mo_future import text, allocate_lock

DEBUG = False
WAIT_FOR_EXPORT = 10  # SECONDS TO WAIT FROM MOST RECENT expect() TO LAST export()

_locker = allocate_lock()
_expectations = []
_expiry = None
_monitor = None
_nothing = object()
_set = object.__setattr__
_get = object.__getattribute__


def expect(*names):
    """

    EXPECT A LATE EXPORT INTO CALLING MODULE

    :param names: MODULE VARIABLES THAT WILL BE FILLED BY ANOTHER MODULE
    :return: PLACEHOLDERS THAT CAN BE USED UNTIL FILL HAPPENS len(output)==len(names)
    """

    if not names:
        _error("expecting at least one name")

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
        for name in names:
            print(">>> " + desc.module.__name__ + " is expecting " + name)

    if len(output) == 1:
        return output[0]
    else:
        return output


class Expecting(object):
    """
    CLASS TO USE AS A MODULE EXPORT PLACEHOLDER UNTIL AN ACTUAL VALUE IS INSERTED
    """

    __slots__ = ["module", "name", "frame"]

    def __init__(self, module, name, frame):
        """
        PLACEHOLDER FOR A LATER VALUE
        :param module:
        :param name:
        :param frame:
        """
        global _monitor, _expiry

        _set(self, "module", module)
        _set(self, "name", name)
        _set(self, "frame", frame)
        with _locker:
            _expiry = time() + WAIT_FOR_EXPORT
            _expectations.append(self)
            if not _monitor:
                _monitor = Thread(target=worker)
                _monitor.start()

    def __call__(self, *args, **kwargs):
        _error(
            'missing expected call export("'
            + self.module.__name__
            + '", '
            + self.name
            + ")"
        )

    def __getattr__(self, item):
        if item in Expecting.__slots__:
            object.__getattribute__(self, item)
        self()

    def __setattr__(self, key, value):
        self()

    def __getitem__(self, item):
        self()

    def __str__(self):
        return "Expect: " + self.module.__name__ + "." + self.name


def export(module, name, value=_nothing):
    """

    MUCH LIKE setattr(module, name, value) BUT WITH CONSISTENCY CHECKS AND MORE CONVENIENCE

    ## COMMON USAGE:

        export("full.path.to.module", value) # read `full.path.to.module.value = value`

    ## RENAME

        export("full.path.to.module", "name", value)  # read `full.path.to.module.name = value`

    ## KNOWN MODULE

        export(myModule, value)  # read `myModule.value = value`


    :param module: MODULE, OR STRING WITH FULL PATH OF MODULE
    :param name: THE VARIABLE TO SET IN MODULE (OR VALUE, IF THERE IS NO NAME CHANGE)
    :param value: (optional) THE VALUE TO ASSIGN
    """

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
            _error("Can not find variable holding a " + value.__class__.__name__)
    if value is _nothing:
        # ASSUME CALLER MODULE IS USED
        frame = inspect.stack()[1]
        value = inspect.getmodule(frame[0])

    desc = getattr(module, name, None)
    if isinstance(desc, Expecting):
        with _locker:
            for i, e in enumerate(_expectations):
                if desc is e:
                    del _expectations[i]
                    break
            else:
                _error(module.__name__ + " is not expecting an export to " + name)
        if DEBUG:
            print(">>> " + module.__name__ + " got expected " + name)
    else:
        _error(module.__name__ + " is not expecting an export to " + name)

    setattr(module, name, value)


def worker():
    global _expectations, _monitor

    if DEBUG:
        print(">>> expectation thread started")
    while True:
        sleep(_expiry - time())
        with _locker:
            if _expiry >= time():
                continue

            _monitor = None
            if not _expectations:
                break

            done, _expectations = _expectations, []

        for d in done:
            sys.stderr.write(
                'missing expected call export("'
                + d.module.__name__
                + '", '
                + d.name
                + ")\n"
            )
        _error("Missing export() calls")

    if DEBUG:
        print(">>> expectation thread ended")


def _error(description):
    raise Exception(description)


def delay_import(module):
    globals = sys._getframe(1).f_globals
    caller_name = globals["__name__"]

    return DelayedImport(caller_name, module)


class DelayedImport(object):

    __slots__ = ["caller", "module"]

    def __init__(self, caller, module):
        _set(self, "caller", caller)
        _set(self, "module", module)

    def _import_now(self):
        # FIND MODULE VARIABLE THAT HOLDS self
        caller_name = _get(self, "caller")
        caller = importlib.import_module(caller_name)
        names = []
        for n in dir(caller):
            try:
                if getattr(caller, n) is self:
                    names.append(n)
            except Exception:
                pass

        if not names:
            _error("Can not find variable holding a " + self.__class__.__name__)

        module = _get(self, "module")
        path = module.split(".")
        module_name, short_name = ".".join(path[:-1]), path[-1]
        try:
            m = importlib.import_module(module_name)
            val = getattr(m, short_name)

            for n in names:
                setattr(caller, n, val)
            return val
        except Exception as cause:
            _error("Can not load " + _get(self, "module") + " caused by " + text(cause))

    def __call__(self, *args, **kwargs):
        m = DelayedImport._import_now(self)
        return m(*args, **kwargs)

    def __getitem__(self, item):
        m = DelayedImport._import_now(self)
        return m[item]

    def __getattribute__(self, item):
        m = DelayedImport._import_now(self)
        return getattr(m, item)
