from pyLibrary.debugs.logs import Log
from pyLibrary.strings import expand_template

_using_mozlog = False

def use_mozlog():
    """
    USE AN INSTANCE OF mozlog.structured.structuredlog.StructuredLogger
    INSTEAD OF THE pyLibrary STANDARD
    WE REPLACE ALL Log ATTRIBUTES WITH ToMozLog ATTRIBUTES
    """
    global _using_mozlog

    if _using_mozlog:
        return

    _using_mozlog = True
    with suppress:
        from pyLibrary.debugs.mozlog.structured import structuredlog


        for n in ["debug", "println", "note", "unexpected", "warning", "error"]:
            copy_attr(n)

        logger = structuredlog.get_default_logger()
        setattr(Log, "moz_logger", logger)


def copy_attr(name):
    setattr(Log, "_old_"+name, getattr(Log, name))
    setattr(Log, name, getattr(ToMozLog, name))



class ToMozLog(object):
    """
    MAP CALLS pyLibrary.debugs.logs.Log TO mozlog.structured.structuredlog.StructuredLogger
    """
    moz_logger = None

    @classmethod
    def debug(cls, template=None, params=None):
        cls.moz_logger.debug(expand_template(template, params))

    @classmethod
    def println(cls, template, params=None):
        cls.moz_logger.debug(expand_template(template, params))

    @classmethod
    def note(cls, template, params=None, stack_depth=0):
        cls.moz_logger.debug(expand_template(template, params))

    @classmethod
    def unexpected(cls, template, params=None, cause=None):
        cls.moz_logger.error(expand_template(template, params))

    @classmethod
    def warning(cls, template, params=None, *args, **kwargs):
        cls.moz_logger.warn(expand_template(template, params))

    @classmethod
    def error(cls, template, params=None, cause=None, stack_depth=0):
        cls.moz_logger.error(expand_template(template, params))
        cls._old_error(template, params, cause, stack_depth)
