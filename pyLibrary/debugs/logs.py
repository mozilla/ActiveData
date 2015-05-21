# encoding: utf-8
#
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
from collections import Mapping

from datetime import datetime
import os
import sys

from pyLibrary.debugs import constants
from pyLibrary.dot import coalesce, Dict, listwrap, wrap, unwrap, unwraplist
from pyLibrary.jsons.encoder import json_encoder
from pyLibrary.thread.threads import Thread, Lock, Queue
from pyLibrary.strings import indent, expand_template


DEBUG_LOGGING = False
ERROR = "ERROR"
WARNING = "WARNING"
UNEXPECTED = "UNEXPECTED"
NOTE = "NOTE"


class Log(object):
    """
    FOR STRUCTURED LOGGING AND EXCEPTION CHAINING
    """
    trace = False
    main_log = None
    logging_multi = None
    profiler = None   # simple pypy-friendly profiler
    cprofiler = None  # screws up with pypy, but better than nothing
    cprofiler_stats = Queue("cprofiler stats")  # ACCUMULATION OF STATS FROM ALL THREADS
    error_mode = False  # prevent error loops

    @classmethod
    def start(cls, settings=None):
        """
        RUN ME FIRST TO SETUP THE THREADED LOGGING
        http://victorlin.me/2012/08/good-logging-practice-in-python/

        log       - LIST OF PARAMETERS FOR LOGGER(S)
        trace     - SHOW MORE DETAILS IN EVERY LOG LINE (default False)
        cprofile  - True==ENABLE THE C-PROFILER THAT COMES WITH PYTHON (default False)
                    USE THE LONG FORM TO SET THE FILENAME {"enabled": True, "filename": "cprofile.tab"}
        profile   - True==ENABLE pyLibrary SIMPLE PROFILING (default False) (eg with Profiler("some description"):)
                    USE THE LONG FORM TO SET FILENAME {"enabled": True, "filename": "profile.tab"}
        constants - UPDATE MODULE CONSTANTS AT STARTUP (PRIMARILY INTENDED TO CHANGE DEBUG STATE)
        """
        if not settings:
            return
        settings = wrap(settings)

        cls.settings = settings
        cls.trace = cls.trace | coalesce(settings.trace, False)
        if cls.trace:
            from pyLibrary.thread.threads import Thread

        if settings.cprofile is True or (isinstance(settings.cprofile, Mapping) and settings.cprofile.enabled):
            if isinstance(settings.cprofile, bool):
                settings.cprofile = {"enabled": True, "filename": "cprofile.tab"}

            import cProfile

            cls.cprofiler = cProfile.Profile()
            cls.cprofiler.enable()

        if settings.profile is True or (isinstance(settings.profile, Mapping) and settings.profile.enabled):
            from pyLibrary.debugs import profiles

            if isinstance(settings.profile, bool):
                profiles.ON = True
                settings.profile = {"enabled": True, "filename": "profile.tab"}

            if settings.profile.enabled:
                profiles.ON = True

        if settings.constants:
            constants.set(settings.constants)

        if not settings.log:
            return

        cls.logging_multi = Log_usingMulti()
        if cls.main_log:
            cls.main_log.stop()
        cls.main_log = Log_usingThread(cls.logging_multi)

        for log in listwrap(settings.log):
            Log.add_log(Log.new_instance(log))

    @classmethod
    def stop(cls):
        from pyLibrary.debugs import profiles

        if cls.cprofiler and hasattr(cls, "settings"):
            import pstats
            cls.cprofiler_stats.add(pstats.Stats(cls.cprofiler))
            write_profile(cls.settings.cprofile, cls.cprofiler_stats.pop_all())

        if profiles.ON and hasattr(cls, "settings"):
            profiles.write(cls.settings.profile)
        cls.main_log.stop()
        cls.main_log = Log_usingStream(sys.stdout)

    @classmethod
    def new_instance(cls, settings):
        settings = wrap(settings)

        if settings["class"]:
            if settings["class"].startswith("logging.handlers."):
                from .log_usingLogger import Log_usingLogger

                return Log_usingLogger(settings)
            else:
                try:
                    from .log_usingLogger import make_log_from_settings

                    return make_log_from_settings(settings)
                except Exception, e:
                    pass  # OH WELL :(

        if settings.log_type == "file" or settings.file:
            return Log_usingFile(settings.file)
        if settings.log_type == "file" or settings.filename:
            return Log_usingFile(settings.filename)
        if settings.log_type == "console":
            from .log_usingThreadedStream import Log_usingThreadedStream
            return Log_usingThreadedStream(sys.stdout)
        if settings.log_type == "stream" or settings.stream:
            from .log_usingThreadedStream import Log_usingThreadedStream
            return Log_usingThreadedStream(settings.stream)
        if settings.log_type == "elasticsearch" or settings.stream:
            from .log_usingElasticSearch import Log_usingElasticSearch
            return Log_usingElasticSearch(settings)
        if settings.log_type == "email":
            from .log_usingEmail import Log_usingEmail
            return Log_usingEmail(settings)

    @classmethod
    def add_log(cls, log):
        cls.logging_multi.add_log(log)

    @classmethod
    def note(cls, template, default_params={}, stack_depth=0, **more_params):
        if len(template) > 10000:
            template = template[:10000]

        params = dict(unwrap(default_params), **more_params)

        log_params = Dict(
            template=template,
            params=params,
            timestamp=datetime.utcnow(),
        )

        if not template.startswith("\n") and template.find("\n") > -1:
            template = "\n" + template

        if cls.trace:
            log_template = "{{timestamp|datetime}} - {{thread.name}} - {{location.file}}:{{location.line}} ({{location.method}}) - " + template.replace("{{", "{{params.")
            f = sys._getframe(stack_depth + 1)
            log_params.location = {
                "line": f.f_lineno,
                "file": f.f_code.co_filename.split(os.sep)[-1],
                "method": f.f_code.co_name
            }
            thread = Thread.current()
            log_params.thread = {"name": thread.name, "id": thread.id}
        else:
            log_template = "{{timestamp|datetime}} - " + template.replace("{{", "{{params.")

        cls.main_log.write(log_template, log_params)

    @classmethod
    def unexpected(cls, template, default_params={}, cause=None, **more_params):
        if isinstance(default_params, BaseException):
            cause = default_params
            default_params = {}

        params = dict(unwrap(default_params), **more_params)

        if cause and not isinstance(cause, Except):
            cause = Except(UNEXPECTED, unicode(cause), trace=extract_tb(0))

        trace = extract_stack(1)
        e = Except(UNEXPECTED, template, params, cause, trace)
        Log.note(
            unicode(e),
            {
                "warning": {
                    "template": template,
                    "params": params,
                    "cause": cause,
                    "trace": trace
                }
            }
        )

    @classmethod
    def alarm(cls, template, params={}, stack_depth=0, **more_params):
        # USE replace() AS POOR MAN'S CHILD TEMPLATE

        template = ("*" * 80) + "\n" + indent(template, prefix="** ").strip() + "\n" + ("*" * 80)
        Log.note(template, params=params, stack_depth=stack_depth + 1, **more_params)

    @classmethod
    def alert(cls, template, params={}, stack_depth=0, **more_params):
        return Log.alarm(template, params, stack_depth+1, **more_params)

    @classmethod
    def warning(
        cls,
        template,
        default_params={},
        cause=None,
        stack_depth=0,       # stack trace offset (==1 if you do not want to report self)
        **more_params
    ):
        if isinstance(default_params, BaseException):
            cause = default_params
            default_params = {}

        params = dict(unwrap(default_params), **more_params)
        cause = unwraplist([Except.wrap(c) for c in listwrap(cause)])
        trace = extract_stack(stack_depth + 1)

        e = Except(WARNING, template, params, cause, trace)
        Log.note(
            unicode(e),
            {
                "warning": {# REDUNDANT INFO
                    "template": template,
                    "params": params,
                    "cause": cause,
                    "trace": trace
                }
            },
            stack_depth=stack_depth + 1
        )


    @classmethod
    def error(
        cls,
        template, # human readable template
        default_params={}, # parameters for template
        cause=None, # pausible cause
        stack_depth=0,        # stack trace offset (==1 if you do not want to report self)
        **more_params
    ):
        """
        raise an exception with a trace for the cause too
        """
        if default_params and isinstance(listwrap(default_params)[0], BaseException):
            cause = default_params
            default_params = {}

        params = dict(unwrap(default_params), **more_params)

        add_to_trace = False
        if cause == None:
            cause = []
        elif isinstance(cause, list):
            pass
        elif isinstance(cause, Except):
            cause = [cause]
        else:
            add_to_trace = True
            if hasattr(cause, "message") and cause.message:
                cause = [Except(ERROR, unicode(cause.message), trace=extract_tb(stack_depth))]
            else:
                cause = [Except(ERROR, unicode(cause), trace=extract_tb(stack_depth))]

        trace = extract_stack(1 + stack_depth)
        if add_to_trace:
            cause[0].trace.extend(trace[1:])

        e = Except(ERROR, template, params, cause, trace)
        raise e

    @classmethod
    def fatal(
        cls,
        template,  # human readable template
        default_params={},  # parameters for template
        cause=None,  # pausible cause
        stack_depth=0,  # stack trace offset (==1 if you do not want to report self)
        **more_params
    ):
        """
        SEND TO STDERR
        """
        if default_params and isinstance(listwrap(default_params)[0], BaseException):
            cause = default_params
            default_params = {}

        params = dict(unwrap(default_params), **more_params)

        if cause == None:
            cause = []
        elif isinstance(cause, list):
            pass
        elif isinstance(cause, Except):
            cause = [cause]
        else:
            cause = [Except(ERROR, unicode(cause), trace=extract_tb(stack_depth))]

        trace = extract_stack(1 + stack_depth)
        e = Except(ERROR, template, params, cause, trace)
        str_e = unicode(e)

        error_mode = cls.error_mode
        try:
            if not error_mode:
                cls.error_mode = True
                Log.note(str_e, {
                    "error": {
                        "template": template,
                        "params": params,
                        "cause": cause,
                        "trace": trace
                    }
                })
        except Exception, f:
            pass
        cls.error_mode = error_mode

        sys.stderr.write(str_e)


    def write(self):
        raise NotImplementedError





def extract_stack(start=0):
    """
    SNAGGED FROM traceback.py
    Extract the raw traceback from the current stack frame.

    Each item in the returned list is a quadruple (filename,
    line number, function name, text), and the entries are in order
    from newest to oldest
    """
    try:
        raise ZeroDivisionError
    except ZeroDivisionError:
        trace = sys.exc_info()[2]
        f = trace.tb_frame.f_back

    for i in range(start):
        f = f.f_back

    stack = []
    n = 0
    while f is not None:
        stack.append({
            "depth": n,
            "line": f.f_lineno,
            "file": f.f_code.co_filename,
            "method": f.f_code.co_name
        })
        f = f.f_back
        n += 1
    return stack


def extract_tb(start):
    """
    SNAGGED FROM traceback.py

    Return list of up to limit pre-processed entries from traceback.

    This is useful for alternate formatting of stack traces.  If
    'limit' is omitted or None, all entries are extracted.  A
    pre-processed stack trace entry is a quadruple (filename, line
    number, function name, text) representing the information that is
    usually printed for a stack trace.
    """
    tb = sys.exc_info()[2]
    for i in range(start):
        tb = tb.tb_next

    trace = []
    n = 0
    while tb is not None:
        f = tb.tb_frame
        trace.append({
            "depth": n,
            "file": f.f_code.co_filename,
            "line": tb.tb_lineno,
            "method": f.f_code.co_name
        })
        tb = tb.tb_next
        n += 1
    trace.reverse()
    return trace


def format_trace(tbs, start=0):
    trace = []
    for d in tbs[start::]:
        item = expand_template('File "{{file}}", line {{line}}, in {{method}}\n', d)
        trace.append(item)
    return "".join(trace)


class Except(Exception):

    @staticmethod
    def new_instance(desc):
        return Except(
            desc.type,
            desc.template,
            desc.params,
            [Except.new_instance(c) for c in listwrap(desc.cause)],
            desc.trace
        )


    def __init__(self, type=ERROR, template=None, params=None, cause=None, trace=None):
        Exception.__init__(self)
        self.type = type
        self.template = template
        self.params = params
        self.cause = cause
        self.trace = trace

    @classmethod
    def wrap(cls, e):
        if e == None:
            return None
        elif isinstance(e, (list, Except)):
            return e
        else:
            if hasattr(e, "message"):
                cause = Except(ERROR, unicode(e.message), trace=extract_tb(0))
            else:
                cause = Except(ERROR, unicode(e), trace=extract_tb(0))

            trace = extract_stack(2)
            cause.trace.extend(trace)
            return cause

    @property
    def message(self):
        return expand_template(self.template, self.params)

    def __contains__(self, value):
        if isinstance(value, basestring):
            if self.message.find(value) >= 0 or self.template.find(value) >= 0:
                return True

        if self.type == value:
            return True
        if self.cause:
            for c in self.cause:
                if value in c:
                    return True
        return False

    def __str__(self):
        output = self.type + ": " + self.template + "\n"
        if self.params:
            output = expand_template(output, self.params)

        if self.trace:
            output += indent(format_trace(self.trace))

        if self.cause:
            cause_strings = []
            for c in listwrap(self.cause):
                try:
                    cause_strings.append(unicode(c))
                except Exception, e:
                    pass

            output += "caused by\n\t" + "and caused by\n\t".join(cause_strings)

        return output

    def __unicode__(self):
        return unicode(str(self))

    def as_dict(self):
        return Dict(
            type=self.type,
            template=self.template,
            params=self.params,
            cause=self.cause,
            trace=self.trace
        )

    def __json__(self):
        return json_encoder(self.as_dict())


class BaseLog(object):
    def write(self, template, params):
        pass

    def stop(self):
        pass


class Log_usingFile(BaseLog):
    def __init__(self, file):
        assert file

        from pyLibrary.env.files import File

        self.file = File(file)
        if self.file.exists:
            self.file.backup()
            self.file.delete()

        self.file_lock = Lock("file lock for logging")

    def write(self, template, params):
        with self.file_lock:
            self.file.append(expand_template(template, params))


class Log_usingThread(BaseLog):

    def __init__(self, logger):
        # DELAYED LOAD FOR THREADS MODULE
        from pyLibrary.thread.threads import Queue

        self.queue = Queue("logs", max=10000, silent=True)
        self.logger = logger

        def worker(please_stop):
            while not please_stop:
                Thread.sleep(1)
                logs = self.queue.pop_all()
                for log in logs:
                    if log is Thread.STOP:
                        if DEBUG_LOGGING:
                            sys.stdout.write("Log_usingThread.worker() sees stop, filling rest of queue\n")
                        please_stop.go()
                    else:
                        self.logger.write(**log)

        self.thread = Thread("log thread", worker)
        self.thread.start()

    def write(self, template, params):
        try:
            self.queue.add({"template": template, "params": params})
            return self
        except Exception, e:
            sys.stdout.write("IF YOU SEE THIS, IT IS LIKELY YOU FORGOT TO RUN Log.start() FIRST\n")
            raise e  # OH NO!

    def stop(self):
        try:
            if DEBUG_LOGGING:
                sys.stdout.write("injecting stop into queue\n")
            self.queue.add(Thread.STOP)  # BE PATIENT, LET REST OF MESSAGE BE SENT
            self.thread.join()
            if DEBUG_LOGGING:
                sys.stdout.write("Log_usingThread telling logger to stop\n")
            self.logger.stop()
        except Exception, e:
            if DEBUG_LOGGING:
                raise e

        try:
            self.queue.close()
        except Exception, f:
            if DEBUG_LOGGING:
                raise f


class Log_usingMulti(BaseLog):
    def __init__(self):
        self.many = []

    def write(self, template, params):
        for m in self.many:
            try:
                m.write(template, params)
            except Exception, e:
                pass
        return self

    def add_log(self, logger):
        self.many.append(logger)
        return self

    def remove_log(self, logger):
        self.many.remove(logger)
        return self

    def clear_log(self):
        self.many = []

    def stop(self):
        for m in self.many:
            try:
                m.stop()
            except Exception, e:
                pass


class Log_usingStream(BaseLog):
    def __init__(self, stream):
        assert stream
        self.stream = stream

    def write(self, template, params):
        value = expand_template(template, params)
        if isinstance(value, unicode):
            value = value.encode('utf8')
        self.stream.write(value)

    def stop(self):
        pass


def write_profile(profile_settings, stats):
    from pyLibrary import convert
    from pyLibrary.env.files import File

    acc = stats[0]
    for s in stats[1:]:
        acc.add(s)

    stats = [{
        "num_calls": d[1],
        "self_time": d[2],
        "total_time": d[3],
        "self_time_per_call": d[2] / d[1],
        "total_time_per_call": d[3] / d[1],
        "file": (f[0] if f[0] != "~" else "").replace("\\", "/"),
        "line": f[1],
        "method": f[2].lstrip("<").rstrip(">")
    }
        for f, d, in acc.stats.iteritems()
    ]
    stats_file = File(profile_settings.filename, suffix=convert.datetime2string(datetime.now(), "_%Y%m%d_%H%M%S"))
    stats_file.write(convert.list2tab(stats))


if not Log.main_log:
    Log.main_log = Log_usingStream(sys.stdout)


