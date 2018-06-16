# encoding: utf-8
#
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

import os
import re
import sys
from collections import Mapping, namedtuple

from mo_dots import Data, coalesce, unwraplist
from mo_files import File
from mo_future import allocate_lock as _allocate_lock, text_type
from mo_kwargs import override
from mo_logs import Log
from mo_logs.exceptions import Except, extract_stack, ERROR, format_trace
from mo_logs.strings import quote
from mo_math.stats import percentile
from mo_threads import Queue, Signal, Thread, Lock, Till
from mo_times import Date, Duration
from mo_times.timer import Timer
from pyLibrary import convert
from pyLibrary.sql import DB, SQL, SQL_TRUE, SQL_FALSE, SQL_NULL, SQL_SELECT, sql_iso

DEBUG = False
TRACE = True

FORMAT_COMMAND = "Running command\n{{command|limit(100)|indent}}"
TOO_LONG_TO_HOLD_TRANSACTION = 10

sqlite3 = None
_load_extension_warning_sent = False
_upgraded = False


class Sqlite(DB):
    """
    Allows multi-threaded access
    Loads extension functions (like SQRT)
    """

    @override
    def __init__(self, filename=None, db=None, get_trace=None, upgrade=True, load_functions=False, kwargs=None):
        """
        :param filename:  FILE TO USE FOR DATABASE
        :param db: AN EXISTING sqlite3 DB YOU WOULD LIKE TO USE (INSTEAD OF USING filename)
        :param get_trace: GET THE STACK TRACE AND THREAD FOR EVERY DB COMMAND (GOOD FOR DEBUGGING)
        :param upgrade: REPLACE PYTHON sqlite3 DLL WITH MORE RECENT ONE, WITH MORE FUNCTIONS (NOT WORKING)
        :param load_functions: LOAD EXTENDED MATH FUNCTIONS (MAY REQUIRE upgrade)
        :param kwargs:
        """
        if upgrade and not _upgraded:
            _upgrade()

        self.settings = kwargs
        self.filename = File(filename).abspath

        # SETUP DATABASE
        DEBUG and Log.note("Sqlite version {{version}}", version=sqlite3.sqlite_version)
        try:
            if db == None:
                self.db = sqlite3.connect(coalesce(self.filename, ':memory:'), check_same_thread=False, isolation_level=None)
            else:
                self.db = db
        except Exception as e:
            Log.error("could not open file {{filename}}", filename=self.filename, cause=e)
        load_functions and self._load_functions()

        self.locker = Lock()
        self.available_transactions = []
        self.queue = Queue("sql commands")   # HOLD (command, result, signal, stacktrace) TUPLES

        self.get_trace = coalesce(get_trace, TRACE)
        self.upgrade = upgrade
        self.closed = False

        # WORKER VARIABLES
        self.transaction_stack = []  # THE TRANSACTION OBJECT WE HAVE PARTIALLY RUN
        self.last_command_item = None  # USE THIS TO HELP BLAME current_transaction FOR HANGING ON TOO LONG
        self.too_long = None
        self.delayed_queries = []
        self.delayed_transactions = []
        self.worker = Thread.run("sqlite db thread", self._worker)

        DEBUG and Log.note("Sqlite version {{version}}", version=self.query("select sqlite_version()").data[0][0])

    def _enhancements(self):
        def regex(pattern, value):
            return 1 if re.match(pattern+"$", value) else 0
        con = self.db.create_function("regex", 2, regex)

        class Percentile(object):
            def __init__(self, percentile):
                self.percentile=percentile
                self.acc=[]

            def step(self, value):
                self.acc.append(value)

            def finalize(self):
                return percentile(self.acc, self.percentile)

        con.create_aggregate("percentile", 2, Percentile)

    def transaction(self):
        thread = Thread.current()
        parent = None
        with self.locker:
            for t in self.available_transactions:
                if t.thread is thread:
                    parent = t

        output = Transaction(self, parent=parent)
        self.available_transactions.append(output)
        return output

    def query(self, command):
        """
        WILL BLOCK CALLING THREAD UNTIL THE command IS COMPLETED
        :param command: COMMAND FOR SQLITE
        :return: list OF RESULTS
        """
        if self.closed:
            Log.error("database is closed")

        signal = _allocate_lock()
        signal.acquire()
        result = Data()
        trace = extract_stack(1) if self.get_trace else None
        self.queue.add(CommandItem(command, result, signal, trace, None))
        signal.acquire()
        if result.exception:
            Log.error("Problem with Sqlite call", cause=result.exception)
        return result

    def close(self):
        """
        OPTIONAL COMMIT-AND-CLOSE
        IF THIS IS NOT DONE, THEN THE THREAD THAT SPAWNED THIS INSTANCE
        :return:
        """
        self.closed = True
        signal = _allocate_lock()
        signal.acquire()
        self.queue.add((COMMIT, None, signal, None))
        signal.acquire()
        self.worker.please_stop.go()
        return

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _load_functions(self):
        global _load_extension_warning_sent
        library_loc = File.new_instance(sys.modules[__name__].__file__, "../..")
        full_path = File.new_instance(library_loc, "vendor/sqlite/libsqlitefunctions.so").abspath
        try:
            trace = extract_stack(0)[0]
            if self.upgrade:
                if os.name == 'nt':
                    file = File.new_instance(trace["file"], "../../vendor/sqlite/libsqlitefunctions.so")
                else:
                    file = File.new_instance(trace["file"], "../../vendor/sqlite/libsqlitefunctions")

                full_path = file.abspath
                self.db.enable_load_extension(True)
                self.db.execute(SQL_SELECT + "load_extension" + sql_iso(quote_value(full_path)))
        except Exception as e:
            if not _load_extension_warning_sent:
                _load_extension_warning_sent = True
                Log.warning("Could not load {{file}}, doing without. (no SQRT for you!)", file=full_path, cause=e)

    def create_new_functions(self):
        def regexp(pattern, item):
            reg = re.compile(pattern)
            return reg.search(item) is not None

        self.db.create_function("REGEXP", 2, regexp)

    def show_warning(self):
        blocked = (self.delayed_queries+self.delayed_transactions)[0]
        blocker = self.last_command_item

        Log.warning(
            "Query for thread {{blocked_thread|quote}} at\n{{blocked_trace|indent}}is blocked by {{blocker_thread|quote}} at\n{{blocker_trace|indent}}this message brought to you by....",
            blocker_thread=blocker.thread.name,
            blocker_trace=format_trace(blocker.trace),
            blocked_thread=blocked.thread.name,
            blocked_trace=format_trace(blocked.trace)
        )

    def _close_transaction(self, command_item):
        query, result, signal, trace, transaction = command_item

        transaction.end_of_life = True
        DEBUG and Log.note(FORMAT_COMMAND, command=query)
        with self.locker:
            self.available_transactions.remove(transaction)
            del self.transaction_stack[-1]
        if not self.transaction_stack:
            # NESTED TRANSACTIONS NOT ALLOWED IN sqlite3
            self.db.execute(query)

        # PUT delayed BACK ON THE QUEUE, IN THE ORDER FOUND, BUT WITH QUERIES FIRST
        if self.too_long is not None:
            with self.too_long.lock:
                self.too_long.job_queue.clear()
        self.too_long = None

        if self.delayed_transactions:
            for c in reversed(self.delayed_transactions):
                self.queue.push(c)
            del self.delayed_transactions[:]
        if self.delayed_queries:
            for c in reversed(self.delayed_queries):
                self.queue.push(c)
            del self.delayed_queries[:]

    def _worker(self, please_stop):
        try:
            # MAIN EXECUTION LOOP
            while not please_stop:
                command_item = self.queue.pop(till=please_stop)
                if command_item is None:
                    break
                self._process_command_item(command_item)
        except Exception as e:
            e = Except.wrap(e)
            if not please_stop:
                Log.warning("Problem with sql thread", cause=e)
        finally:
            self.closed = True
            DEBUG and Log.note("Database is closed")
            self.db.close()

    def _process_command_item(self, command_item):
        query, result, signal, trace, transaction = command_item

        with Timer("SQL Timing", debug=DEBUG):
            if transaction is None:
                # THIS IS A TRANSACTIONLESS QUERY, DELAY IT IF THERE IS A CURRENT TRANSACTION
                if self.transaction_stack:
                    if self.too_long is None:
                        self.too_long = Till(seconds=TOO_LONG_TO_HOLD_TRANSACTION)
                        self.too_long.on_go(self.show_warning)
                    self.delayed_queries.append(command_item)
                    return
            elif self.transaction_stack and self.transaction_stack[-1] not in [transaction, transaction.parent]:
                # THIS TRANSACTION IS NOT THE CURRENT TRANSACTION, DELAY IT
                if self.too_long is None:
                    self.too_long = Till(seconds=TOO_LONG_TO_HOLD_TRANSACTION)
                    self.too_long.on_go(self.show_warning)
                self.delayed_transactions.append(command_item)
                return
            else:
                # ENSURE THE CURRENT TRANSACTION IS UP TO DATE FOR THIS query
                if not self.transaction_stack:
                    # sqlite3 ALLOWS ONLY ONE TRANSACTION AT A TIME
                    self.db.execute(BEGIN)
                    self.transaction_stack.append(transaction)
                elif transaction != self.transaction_stack[-1]:
                    self.transaction_stack.append(transaction)
                elif transaction.exception:
                    result.exception = Except(
                        type=ERROR,
                        template="Not allowed to continue using a transaction that failed",
                        cause=transaction.exception,
                        trace=trace
                    )
                    signal.release()
                    return

                try:
                    transaction.do_all()
                except Exception as e:
                    # DEAL WITH ERRORS IN QUEUED COMMANDS
                    # WE WILL UNWRAP THE OUTER EXCEPTION TO GET THE CAUSE
                    err = Except(
                        type=ERROR,
                        template="Bad call to Sqlite3 while "+FORMAT_COMMAND,
                        params={"command": e.params.current.command},
                        cause=e.cause,
                        trace=e.params.current.trace
                    )
                    transaction.exception = result.exception = err

                    if query in [COMMIT, ROLLBACK]:
                        self._close_transaction(CommandItem(ROLLBACK, result, signal, trace, transaction))

                    signal.release()
                    return

            try:
                # DEAL WITH END-OF-TRANSACTION MESSAGES
                if query in [COMMIT, ROLLBACK]:
                    self._close_transaction(command_item)
                    return

                # EXECUTE QUERY
                self.last_command_item = command_item
                DEBUG and Log.note(FORMAT_COMMAND, command=query)
                curr = self.db.execute(query)
                result.meta.format = "table"
                result.header = [d[0] for d in curr.description] if curr.description else None
                result.data = curr.fetchall()
                if DEBUG and result.data:
                    text = convert.table2csv(list(result.data))
                    Log.note("Result:\n{{data|limit(100)|indent}}", data=text)
            except Exception as e:
                e = Except.wrap(e)
                err = Except(
                    type=ERROR,
                    template="Bad call to Sqlite while " + FORMAT_COMMAND,
                    params={"command": query},
                    trace=trace,
                    cause=e
                )
                result.exception = err
                if transaction:
                    transaction.exception = err
            finally:
                signal.release()


class Transaction(object):

    def __init__(self, db, parent=None):
        self.db = db
        self.locker = Lock("transaction " + text_type(id(self)) + " todo lock")
        self.todo = []
        self.complete = 0
        self.end_of_life = False
        self.exception = None
        self.parent = parent
        self.thread = parent.thread if parent else Thread.current()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        causes = []
        try:
            if isinstance(exc_val, Exception):
                causes.append(Except.wrap(exc_val))
                self.rollback()
            else:
                self.commit()
        except Exception as e:
            causes.append(Except.wrap(e))
            Log.error("Transaction failed", cause=unwraplist(causes))

    def transaction(self):
        with self.db.locker:
            output = Transaction(self.db, parent=self)
            self.db.available_transactions.append(output)
        return output

    def execute(self, command):
        trace = extract_stack(1) if self.db.get_trace else None
        with self.locker:
            self.todo.append(CommandItem(command, None, None, trace, self))

    def do_all(self):
        # ENSURE PARENT TRANSACTION IS UP TO DATE
        if self.parent:
            self.parent.do_all()

        try:
            # GET THE REMAINING COMMANDS
            with self.locker:
                todo = self.todo[self.complete:]
                self.complete = len(self.todo)

            # RUN THEM
            for c in todo:
                DEBUG and Log.note(FORMAT_COMMAND, command=c.command)
                self.db.db.execute(c.command)
                if c.command in [COMMIT, ROLLBACK]:
                    Log.error("logic error")
        except Exception as e:
            Log.error("problem running commands", current=c, cause=e)


    def query(self, query):
        if self.db.closed:
            Log.error("database is closed")

        signal = _allocate_lock()
        signal.acquire()
        result = Data()
        trace = extract_stack(1) if self.db.get_trace else None
        self.db.queue.add(CommandItem(query, result, signal, trace, self))
        signal.acquire()
        if result.exception:
            Log.error("Problem with Sqlite call", cause=result.exception)
        return result

    def rollback(self):
        self.query(ROLLBACK)

    def commit(self):
        self.query(COMMIT)


CommandItem = namedtuple("CommandItem", ("command", "result", "is_done", "trace", "transaction"))


_no_need_to_quote = re.compile(r"^\w+$", re.UNICODE)


def quote_column(column_name, table=None):
    if isinstance(column_name, SQL):
        return column_name

    if not isinstance(column_name, text_type):
        Log.error("expecting a name")
    if table != None:
        return SQL(" d" + quote(table) + "." + quote(column_name) + " ")
    else:
        if _no_need_to_quote.match(column_name):
            return SQL(" " + column_name + " ")
        return SQL(" " + quote(column_name) + " ")


def quote_value(value):
    if isinstance(value, (Mapping, list)):
        return SQL(".")
    elif isinstance(value, Date):
        return SQL(text_type(value.unix))
    elif isinstance(value, Duration):
        return SQL(text_type(value.seconds))
    elif isinstance(value, text_type):
        return SQL("'" + value.replace("'", "''") + "'")
    elif value == None:
        return SQL_NULL
    elif value is True:
        return SQL_TRUE
    elif value is False:
        return SQL_FALSE
    else:
        return SQL(text_type(value))


def join_column(a, b):
    a = quote_column(a)
    b = quote_column(b)
    return SQL(a.template.rstrip() + "." + b.template.lstrip())


BEGIN = "BEGIN"
COMMIT = "COMMIT"
ROLLBACK = "ROLLBACK"


def _upgrade():
    global _upgraded
    global sqlite3

    try:
        Log.note("sqlite not upgraded")
        # return
        #
        # import sys
        # import platform
        # if "windows" in platform.system().lower():
        #     original_dll = File.new_instance(sys.exec_prefix, "dlls/sqlite3.dll")
        #     if platform.architecture()[0]=='32bit':
        #         source_dll = File("vendor/pyLibrary/vendor/sqlite/sqlite3_32.dll")
        #     else:
        #         source_dll = File("vendor/pyLibrary/vendor/sqlite/sqlite3_64.dll")
        #
        #     if not all(a == b for a, b in zip_longest(source_dll.read_bytes(), original_dll.read_bytes())):
        #         original_dll.backup()
        #         File.copy(source_dll, original_dll)
        # else:
        #     pass
    except Exception as e:
        Log.warning("could not upgrade python's sqlite", cause=e)

    import sqlite3
    _ = sqlite3
    _upgraded = True
