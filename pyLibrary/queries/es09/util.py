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
from __future__ import absolute_import
from copy import deepcopy
from datetime import datetime

from pyLibrary import convert
from pyLibrary import strings
from pyLibrary.collections import COUNT
from pyLibrary.maths import stats
from pyLibrary.debugs.logs import Log
from pyLibrary.maths import Math
from pyLibrary.queries import domains
from pyLibrary.dot.dicts import Dict
from pyLibrary.dot import split_field, join_field, coalesce
from pyLibrary.dot.lists import DictList
from pyLibrary.dot import wrap
from pyLibrary.queries import qb
from pyLibrary.queries.es09 import expressions
from pyLibrary.queries.es09.expressions import value2MVEL, isKeyword
from pyLibrary.queries.expressions import simplify_esfilter
from pyLibrary.times import durations


TrueFilter = {"match_all": {}}
DEBUG = False

INDEX_CACHE = {}  # MATCH NAMES TO ES URL AND COLUMNS eg {name:{"url":url, "columns":columns"}}


def post(es, FromES, limit):
    if not FromES.facets and FromES.size == 0 and not FromES.aggs:
        Log.error("FromES is sending no facets")
        # DO NOT KNOW WHY THIS WAS HERE
    # if isinstance(query.select, list) or len(query.edges) and not FromES.facets.keys and FromES.size == 0:
    #     Log.error("FromES is sending no facets")

    postResult = None
    try:
        postResult = es.search(FromES)

        for facetName, f in postResult.facets.items():
            if f._type == "statistical":
                continue
            if not f.terms:
                continue

            if not DEBUG and not limit and len(f.terms) == limit:
                Log.error("Not all data delivered (" + str(len(f.terms)) + "/" + str(f.total) + ") try smaller range")
    except Exception, e:
        Log.error("Error with FromES", e)

    return postResult


def build_es_query(query):
    output = wrap({
        "query": {"match_all": {}},
        "from": 0,
        "size": 100 if DEBUG else 0,
        "sort": [],
        "facets": {
        }
    })

    if DEBUG:
        # TO LIMIT RECORDS TO WHAT'S IN FACETS
        output.query = {
            "filtered": {
                "query": {
                    "match_all": {}
                },
                "filter": simplify_esfilter(query.where)
            }
        }

    return output


def parse_columns(parent_path, esProperties):
    """
    RETURN THE COLUMN DEFINITIONS IN THE GIVEN esProperties OBJECT
    """
    columns = DictList()
    for name, property in esProperties.items():
        if parent_path:
            path = join_field(split_field(parent_path) + [name])
        else:
            path = name

        if property.type == "nested" and property.properties:
            # NESTED TYPE IS A NEW TYPE DEFINITION
            # MARKUP CHILD COLUMNS WITH THE EXTRA DEPTH
            child_columns = deepcopy(parse_columns(path, property.properties))
            self_columns = deepcopy(child_columns)
            for c in self_columns:
                c.depth += 1
            columns.extend(self_columns)
            columns.append({
                "name": join_field(split_field(path)[1::]),
                "type": "nested",
                "useSource": False
            })

            if path not in INDEX_CACHE:
                pp = split_field(parent_path)
                for i in qb.reverse(range(len(pp))):
                    c = INDEX_CACHE.get(join_field(pp[:i + 1]), None)
                    if c:
                        INDEX_CACHE[path] = c.copy()
                        break
                else:
                    Log.error("Can not find parent")

                INDEX_CACHE[path].name = path
            INDEX_CACHE[path].columns = child_columns
            continue

        if property.properties:
            child_columns = parse_columns(path, property.properties)
            columns.extend(child_columns)
            columns.append({
                "name": join_field(split_field(path)[1::]),
                "type": "object",
                "useSource": False
            })

        if property.dynamic:
            continue
        if not property.type:
            continue
        if property.type == "multi_field":
            property.type = property.fields[name].type  # PULL DEFAULT TYPE
            for i, n, p in enumerate(property.fields):
                if n == name:
                    # DEFAULT
                    columns.append({"name": join_field(split_field(path)[1::]), "type": p.type, "useSource": p.index == "no"})
                else:
                    columns.append({"name": join_field(split_field(path)[1::]) + "\\." + n, "type": p.type, "useSource": p.index == "no"})
            continue

        if property.type in ["string", "boolean", "integer", "date", "long", "double"]:
            columns.append({
                "name": join_field(split_field(path)[1::]),
                "type": property.type,
                "useSource": property.index == "no"
            })
            if property.index_name and name != property.index_name:
                columns.append({
                    "name": property.index_name,
                    "type": property.type,
                    "useSource": property.index == "no"
                })
        elif property.enabled == None or property.enabled == False:
            columns.append({
                "name": join_field(split_field(path)[1::]),
                "type": "object",
                "useSource": True
            })
        else:
            Log.warning("unknown type {{type}} for property {{path}}",  type= property.type,  path= path)

    return columns


def compileTime2Term(edge):
    """
    RETURN MVEL CODE THAT MAPS TIME AND DURATION DOMAINS DOWN TO AN INTEGER AND
    AND THE JAVASCRIPT THAT WILL TURN THAT INTEGER BACK INTO A PARTITION (INCLUDING NULLS)
    """
    if edge.esscript:
        Log.error("edge script not supported yet")

    # IS THERE A LIMIT ON THE DOMAIN?
    numPartitions = len(edge.domain.partitions)
    value = edge.value
    if isKeyword(value):
        value = "doc[\"" + value + "\"].value"

    nullTest = compileNullTest(edge)
    ref = coalesce(edge.domain.min, edge.domain.max, datetime(2000, 1, 1))

    if edge.domain.interval.month > 0:
        offset = ref.subtract(ref.floorMonth(), durations.DAY).milli
        if offset > durations.DAY.milli * 28:
            offset = ref.subtract(ref.ceilingMonth(), durations.DAY).milli
        partition2int = "milli2Month(" + value + ", " + value2MVEL(offset) + ")"
        partition2int = "((" + nullTest + ") ? 0 : " + partition2int + ")"

        def int2Partition(value):
            if Math.round(value) == 0:
                return edge.domain.NULL

            d = datetime(str(value)[:4:], str(value).right(2), 1)
            d = d.addMilli(offset)
            return edge.domain.getPartByKey(d)
    else:
        partition2int = "Math.floor((" + value + "-" + value2MVEL(ref) + ")/" + edge.domain.interval.milli + ")"
        partition2int = "((" + nullTest + ") ? " + numPartitions + " : " + partition2int + ")"

        def int2Partition(value):
            if Math.round(value) == numPartitions:
                return edge.domain.NULL
            return edge.domain.getPartByKey(ref.add(edge.domain.interval.multiply(value)))

    return Dict(toTerm={"head": "", "body": partition2int}, fromTerm=int2Partition)


# RETURN MVEL CODE THAT MAPS DURATION DOMAINS DOWN TO AN INTEGER AND
# AND THE JAVASCRIPT THAT WILL TURN THAT INTEGER BACK INTO A PARTITION (INCLUDING NULLS)
def compileDuration2Term(edge):
    if edge.esscript:
        Log.error("edge script not supported yet")

    # IS THERE A LIMIT ON THE DOMAIN?
    numPartitions = len(edge.domain.partitions)
    value = edge.value
    if isKeyword(value):
        value = "doc[\"" + value + "\"].value"

    ref = coalesce(edge.domain.min, edge.domain.max, durations.ZERO)
    nullTest = compileNullTest(edge)

    ms = edge.domain.interval.milli
    if edge.domain.interval.month > 0:
        ms = durations.YEAR.milli / 12 * edge.domain.interval.month

    partition2int = "Math.floor((" + value + "-" + value2MVEL(ref) + ")/" + ms + ")"
    partition2int = "((" + nullTest + ") ? " + numPartitions + " : " + partition2int + ")"

    def int2Partition(value):
        if Math.round(value) == numPartitions:
            return edge.domain.NULL
        return edge.domain.getPartByKey(ref.add(edge.domain.interval.multiply(value)))

    return Dict(toTerm={"head": "", "body": partition2int}, fromTerm=int2Partition)


# RETURN MVEL CODE THAT MAPS THE numeric DOMAIN DOWN TO AN INTEGER AND
# AND THE JAVASCRIPT THAT WILL TURN THAT INTEGER BACK INTO A PARTITION (INCLUDING NULLS)
def compileNumeric2Term(edge):
    if edge.script:
        Log.error("edge script not supported yet")

    if edge.domain.type != "numeric" and edge.domain.type != "count":
        Log.error("can only translate numeric domains")

    numPartitions = len(edge.domain.partitions)
    value = edge.value
    if isKeyword(value):
        value = "doc[\"" + value + "\"].value"

    if not edge.domain.max:
        if not edge.domain.min:
            ref = 0
            partition2int = "Math.floor(" + value + ")/" + value2MVEL(edge.domain.interval) + ")"
            nullTest = "false"
        else:
            ref = value2MVEL(edge.domain.min)
            partition2int = "Math.floor((" + value + "-" + ref + ")/" + value2MVEL(edge.domain.interval) + ")"
            nullTest = "" + value + "<" + ref
    elif not edge.domain.min:
        ref = value2MVEL(edge.domain.max)
        partition2int = "Math.floor((" + value + "-" + ref + ")/" + value2MVEL(edge.domain.interval) + ")"
        nullTest = "" + value + ">=" + ref
    else:
        top = value2MVEL(edge.domain.max)
        ref = value2MVEL(edge.domain.min)
        partition2int = "Math.floor((" + value + "-" + ref + ")/" + value2MVEL(edge.domain.interval) + ")"
        nullTest = "(" + value + "<" + ref + ") or (" + value + ">=" + top + ")"

    partition2int = "((" + nullTest + ") ? " + numPartitions + " : " + partition2int + ")"
    offset = convert.value2int(ref)

    def int2Partition(value):
        if Math.round(value) == numPartitions:
            return edge.domain.NULL
        return edge.domain.getPartByKey((value * edge.domain.interval) + offset)

    return Dict(toTerm={"head": "", "body": partition2int}, fromTerm=int2Partition)


def compileString2Term(edge):
    if edge.esscript:
        Log.error("edge script not supported yet")

    value = edge.value
    if isKeyword(value):
        value = strings.expand_template("getDocValue({{path}})", {"path": convert.string2quote(value)})
    else:
        Log.error("not handled")

    def fromTerm(value):
        return edge.domain.getPartByKey(value)

    return Dict(
        toTerm={"head": "", "body": value},
        fromTerm=fromTerm
    )


def compileNullTest(edge):
    """
    RETURN A MVEL EXPRESSION THAT WILL EVALUATE TO true FOR OUT-OF-BOUNDS
    """
    if edge.domain.type not in domains.ALGEBRAIC:
        Log.error("can only translate time and duration domains")

    # IS THERE A LIMIT ON THE DOMAIN?
    value = edge.value
    if isKeyword(value):
        value = "doc[\"" + value + "\"].value"

    if not edge.domain.max:
        if not edge.domain.min:
            return False
        bot = value2MVEL(edge.domain.min)
        nullTest = "" + value + "<" + bot
    elif not edge.domain.min:
        top = value2MVEL(edge.domain.max)
        nullTest = "" + value + ">=" + top
    else:
        top = value2MVEL(edge.domain.max)
        bot = value2MVEL(edge.domain.min)
        nullTest = "(" + value + "<" + bot + ") or (" + value + ">=" + top + ")"

    return nullTest


def compileEdges2Term(mvel_compiler, edges, constants):
    """
    TERMS ARE ALWAYS ESCAPED SO THEY CAN BE COMPOUNDED WITH PIPE (|)

    GIVE MVEL CODE THAT REDUCES A UNIQUE TUPLE OF PARTITIONS DOWN TO A UNIQUE TERM
    GIVE LAMBDA THAT WILL CONVERT THE TERM BACK INTO THE TUPLE
    RETURNS TUPLE OBJECT WITH "type" and "value" ATTRIBUTES.
    "type" CAN HAVE A VALUE OF "script", "field" OR "count"
    CAN USE THE constants (name, value pairs)
    """

    # IF THE QUERY IS SIMPLE ENOUGH, THEN DO NOT USE TERM PACKING
    edge0 = edges[0]

    if len(edges) == 1 and edge0.domain.type in ["set", "default"]:
        # THE TERM RETURNED WILL BE A MEMBER OF THE GIVEN SET
        def temp(term):
            return DictList([edge0.domain.getPartByKey(term)])

        if edge0.value and isKeyword(edge0.value):
            return Dict(
                field=edge0.value,
                term2parts=temp
            )
        elif COUNT(edge0.domain.dimension.fields) == 1:
            return Dict(
                field=edge0.domain.dimension.fields[0],
                term2parts=temp
            )
        elif not edge0.value and edge0.domain.partitions:
            script = mvel_compiler.Parts2TermScript(edge0.domain)
            return Dict(
                expression=script,
                term2parts=temp
            )
        else:
            return Dict(
                expression=mvel_compiler.compile_expression(edge0.value, constants),
                term2parts=temp
            )

    mvel_terms = []     # FUNCTION TO PACK TERMS
    fromTerm2Part = []  # UNPACK TERMS BACK TO PARTS
    for e in edges:
        domain = e.domain
        fields = domain.dimension.fields

        if not e.value and fields:
            code, decode = mvel_compiler.Parts2Term(e.domain)
            t = Dict(
                toTerm=code,
                fromTerm=decode
            )
        elif fields:
            Log.error("not expected")
        elif e.domain.type == "time":
            t = compileTime2Term(e)
        elif e.domain.type == "duration":
            t = compileDuration2Term(e)
        elif e.domain.type in domains.ALGEBRAIC:
            t = compileNumeric2Term(e)
        elif e.domain.type == "set" and not fields:
            def fromTerm(term):
                return e.domain.getPartByKey(term)

            code, decode = mvel_compiler.Parts2Term(e.domain)
            t = Dict(
                toTerm=code,
                fromTerm=decode
            )
        else:
            t = compileString2Term(e)

        if not t.toTerm.body:
            mvel_compiler.Parts2Term(e.domain)
            Log.unexpected("what?")

        fromTerm2Part.append(t.fromTerm)
        mvel_terms.append(t.toTerm.body)

    # REGISTER THE DECODE FUNCTION
    def temp(term):
        terms = term.split('|')

        output = DictList([t2p(t) for t, t2p in zip(terms, fromTerm2Part)])
        return output

    return Dict(
        expression=mvel_compiler.compile_expression("+'|'+".join(mvel_terms), constants),
        term2parts=temp
    )


def fix_es_stats(s):
    """
    ES RETURNS BAD DEFAULT VALUES FOR STATS
    """
    s = wrap(s)
    if s.count == 0:
        return stats.zero
    return s


# MAP NAME TO SQL FUNCTION
aggregates = {
    "none": "none",
    "one": "count",
    "sum": "total",
    "add": "total",
    "count": "count",
    "maximum": "max",
    "minimum": "min",
    "max": "max",
    "min": "min",
    "mean": "mean",
    "average": "mean",
    "avg": "mean",
    "N": "count",
    "X0": "count",
    "X1": "total",
    "X2": "sum_of_squares",
    "std": "std_deviation",
    "stddev": "std_deviation",
    "var": "variance",
    "variance": "variance"
}


