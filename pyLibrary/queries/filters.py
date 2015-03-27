# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from __future__ import division

from pyLibrary.collections import OR
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap


TRUE_FILTER = True
FALSE_FILTER = False


def simplify_esfilter(esfilter):
    try:
        output = normalize_esfilter(where2esfilter(esfilter))
        if output is TRUE_FILTER:
            return {"match_all": {}}
        output.isNormal = None
        return output
    except Exception, e:
        from pyLibrary.debugs.logs import Log

        Log.unexpected("programmer error", e)



def removeOr(esfilter):
    if esfilter["not"]:
        return {"not": removeOr(esfilter["not"])}

    if esfilter["and"]:
        return {"and": [removeOr(v) for v in esfilter["and"]]}

    if esfilter["or"]:  # CONVERT OR TO NOT.AND.NOT
        return {"not": {"and": [{"not": removeOr(v)} for v in esfilter["or"]]}}

    return esfilter

def normalize_esfilter(esfilter):
    """
    SIMPLFY THE LOGIC EXPRESSION
    """
    return wrap(_normalize(wrap(esfilter)))



def _normalize(esfilter):
    """
    TODO: DO NOT USE Dicts, WE ARE SPENDING TOO MUCH TIME WRAPPING/UNWRAPPING
    REALLY, WE JUST COLLAPSE CASCADING and AND or FILTERS
    """
    if esfilter is TRUE_FILTER or esfilter is FALSE_FILTER or esfilter.isNormal:
        return esfilter

    # Log.note("from: " + convert.value2json(esfilter))
    isDiff = True

    while isDiff:
        isDiff = False

        if esfilter["and"] != None:
            output = []
            for a in esfilter["and"]:
                if isinstance(a, (list, set)):
                    from pyLibrary.debugs.logs import Log
                    Log.error("and clause is not allowed a list inside a list")
                a_ = normalize_esfilter(a)
                if a_ is not a:
                    isDiff = True
                a = a_
                if a == TRUE_FILTER:
                    isDiff = True
                    continue
                if a == FALSE_FILTER:
                    return FALSE_FILTER
                if a.get("and"):
                    isDiff = True
                    a.isNormal = None
                    output.extend(a.get("and"))
                else:
                    a.isNormal = None
                    output.append(a)
            if not output:
                return TRUE_FILTER
            elif len(output) == 1:
                # output[0].isNormal = True
                esfilter = output[0]
                break
            elif isDiff:
                esfilter = wrap({"and": output})
            continue

        if esfilter["or"] != None:
            output = []
            for a in esfilter["or"]:
                a_ = _normalize(a)
                if a_ is not a:
                    isDiff = True
                a = a_

                if a == TRUE_FILTER:
                    return TRUE_FILTER
                if a == FALSE_FILTER:
                    isDiff = True
                    continue
                if a.get("or"):
                    a.isNormal = None
                    isDiff = True
                    output.extend(a["or"])
                else:
                    a.isNormal = None
                    output.append(a)
            if not output:
                return FALSE_FILTER
            elif len(output) == 1:
                esfilter = output[0]
                break
            elif isDiff:
                esfilter = wrap({"or": output})
            continue

        if esfilter.term != None:
            if esfilter.term.keys():
                esfilter.isNormal = True
                return esfilter
            else:
                return TRUE_FILTER

        if esfilter.terms != None:
            for k, v in esfilter.terms.items():
                if len(v) > 0:
                    if OR(vv == None for vv in v):
                        rest = [vv for vv in v if vv != None]
                        if len(rest) > 0:
                            return {
                                "or": [
                                    {"missing": {"field": k}},
                                    {"terms": {k: rest}}
                                ],
                                "isNormal": True
                            }
                        else:
                            return {
                                "missing": {"field": k},
                                "isNormal": True
                            }
                    else:
                        esfilter.isNormal = True
                        return esfilter
            return FALSE_FILTER

        if esfilter["not"] != None:
            _sub = esfilter["not"]
            sub = _normalize(_sub)
            if sub is FALSE_FILTER:
                return TRUE_FILTER
            elif sub is TRUE_FILTER:
                return FALSE_FILTER
            elif sub is not _sub:
                sub.isNormal = None
                return wrap({"not": sub, "isNormal": True})
            else:
                sub.isNormal = None

    esfilter.isNormal = True
    return esfilter


def where2esfilter(where):
    """
    CONVERT qb QUERY where CLAUSE TO ELASTICSEARCH FILTER FORMAT
    """
    if where is True or where == None:
        return {"match_all": {}}
    if where is False:
        return False

    k, v = where.items()[0]
    return converter_map.get(k, _no_convert)(k, v)


def _convert_many(k, v):
    return {k: [where2esfilter(vv) for vv in v]}


def _convert_not(k, v):
    return {k: where2esfilter(v)}


def _convert_not_equal(op, term):
    if isinstance(term, list):
        Log.error("the 'ne' clause does not accept a list parameter")

    var, val = term.items()[0]
    if isinstance(val, list):
        return {"not": {"terms": term}}
    else:
        return {"not": {"term": term}}


def _convert_in(op, term):
    if not term:
        Log.error("Expecting a term")
    var, val = term.items()[0]

    if isinstance(val, list):
        v2 = [vv for vv in val if vv != None]

        if len(v2) == 0:
            if len(val) == 0:
                return False
            else:
                return {"missing": {"field": var}}

        if len(v2) == 1:
            output = {"term": {var: v2[0]}}
        else:
            output = {"terms": {var: v2}}

        if len(v2) != len(val):
            output = {"or": [
                {"missing": {"field": var}},
                output
            ]}
        return output
    else:
        return {"term": term}


def _convert_inequality(ine, term):
    var, val = term.items()[0]
    return {"range": {var: {ine: val}}}


def _no_convert(op, term):
    return {op: term}


def _convert_field(k, var):
    if isinstance(var, basestring):
        return {k: {"field": var}}
    if isinstance(var, dict) and var.get("field"):
        return {k: var}
    Log.error("do not know how to handle {{value}}", {"value": {k: var}})


converter_map = {
    "and": _convert_many,
    "or": _convert_many,
    "not": _convert_not,
    "term": _convert_in,
    "terms": _convert_in,
    "eq": _convert_in,
    "ne": _convert_not_equal,
    "in": _convert_in,
    "missing": _convert_field,
    "exists": _convert_field,
    "gt": _convert_inequality,
    "gte": _convert_inequality,
    "lt": _convert_inequality,
    "lte": _convert_inequality
}


