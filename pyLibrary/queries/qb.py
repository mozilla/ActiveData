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
import __builtin__
from collections import Mapping
from types import GeneratorType

from pyLibrary import dot, convert
from pyLibrary.collections import UNION, MIN
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import set_default, Null, Dict, split_field, coalesce, join_field
from pyLibrary.dot.lists import DictList
from pyLibrary.dot import listwrap, wrap, unwrap
from pyLibrary.dot.objects import DictClass, DictObject
from pyLibrary.maths import Math
from pyLibrary.queries import flat_list, query, group_by
from pyLibrary.queries.container import Container
from pyLibrary.queries.cubes.aggs import cube_aggs
from pyLibrary.queries.expressions import TRUE_FILTER, FALSE_FILTER, compile_expression, qb_expression_to_function
from pyLibrary.queries.flat_list import FlatList
from pyLibrary.queries.index import Index
from pyLibrary.queries.query import Query, _normalize_selects, sort_direction, _normalize_select
from pyLibrary.queries.cube import Cube
from pyLibrary.queries.unique_index import UniqueIndex

# A COLLECTION OF DATABASE OPERATORS (RELATIONAL ALGEBRA OPERATORS)
# qb QUERY DOCUMENTATION: https://github.com/klahnakoski/qb/tree/master/docs
# START HERE: https://github.com/klahnakoski/qb/blob/master/docs/Qb_Reference.md
# TODO: USE http://docs.sqlalchemy.org/en/latest/core/tutorial.html AS DOCUMENTATION FRAMEWORK


def run(query):
    """
    THIS FUNCTION IS SIMPLY SWITCHING BASED ON THE query["from"] CONTAINER,
    BUT IT IS ALSO PROCESSING A list CONTAINER; SEPARATE TO A ListContainer
    """
    query = Query(query)
    frum = query["from"]
    if isinstance(frum, Container):
        with frum:
            return frum.query(query)
    elif isinstance(frum, (list, set, GeneratorType)):
        frum = wrap(list(frum))
    elif isinstance(frum, Cube):
        if is_aggs(query):
            return cube_aggs(frum, query)

    elif isinstance(frum, Query):
        frum = run(frum).data
    else:
        Log.error("Do not know how to handle {{type}}",  type=frum.__class__.__name__)

    if is_aggs(query):
        frum = list_aggs(frum, query)
    else:  # SETOP
        try:
            if query.filter != None or query.esfilter != None:
                Log.error("use 'where' clause")
        except AttributeError, e:
            pass

        if query.where is not TRUE_FILTER:
            frum = filter(frum, query.where)

        if query.sort:
            frum = sort(frum, query.sort)

        if query.select:
            frum = select(frum, query.select)

    if query.window:
        if isinstance(frum, Cube):
            frum = list(frum.values())

        for param in query.window:
            window(frum, param)

    # AT THIS POINT frum IS IN LIST FORMAT, NOW PACKAGE RESULT
    if query.format == "table":
        frum = convert.list2table(frum)
        frum.meta.format = "table"
    else:
        frum = wrap({
            "meta": {"format": "list"},
            "data": frum
        })

    return frum


groupby = group_by.groupby


def index(data, keys=None):
# return dict that uses keys to index data
    o = Index(keys)

    if isinstance(data, Cube):
        if data.edges[0].name==keys[0]:
            #QUICK PATH
            names = list(data.data.keys())
            for d in (set_default(dot.zip(names, r), {keys[0]: p}) for r, p in zip(zip(*data.data.values()), data.edges[0].domain.partitions.value)):
                o.add(d)
            return o
        else:
            Log.error("Can not handle indexing cubes at this time")

    for d in data:
        o.add(d)
    return o


def unique_index(data, keys=None, fail_on_dup=True):
    """
    RETURN dict THAT USES KEYS TO INDEX DATA
    ONLY ONE VALUE ALLOWED PER UNIQUE KEY
    """
    o = UniqueIndex(listwrap(keys), fail_on_dup=fail_on_dup)

    for d in data:
        try:
            o.add(d)
        except Exception, e:
            o.add(d)
            Log.error("index {{index}} is not unique {{key}} maps to both {{value1}} and {{value2}}",
                index= keys,
                key= select([d], keys)[0],
                value1= o[d],
                value2= d,
                cause=e
            )
    return o


def map2set(data, relation):
    """
    EXPECTING A isinstance(relation, Mapping) THAT MAPS VALUES TO lists
    THE LISTS ARE EXPECTED TO POINT TO MEMBERS OF A SET
    A set() IS RETURNED
    """
    if data == None:
        return Null
    if isinstance(relation, Dict):
        Log.error("Does not accept a Dict")

    if isinstance(relation, Mapping):
        try:
            # relation[d] is expected to be a list
            # return set(cod for d in data for cod in relation[d])
            output = set()
            for d in data:
                for cod in relation.get(d, []):
                    output.add(cod)
            return output
        except Exception, e:
            Log.error("Expecting a dict with lists in codomain", e)
    else:
        try:
            # relation[d] is expected to be a list
            # return set(cod for d in data for cod in relation[d])
            output = set()
            for d in data:
                cod = relation(d)
                if cod == None:
                    continue
                output.add(cod)
            return output
        except Exception, e:
            Log.error("Expecting a dict with lists in codomain", e)
    return Null


def tuple(data, field_name):
    """
    RETURN LIST  OF TUPLES
    """
    if isinstance(data, Cube):
        Log.error("not supported yet")

    if isinstance(data, FlatList):
        Log.error("not supported yet")

    if isinstance(field_name, Mapping) and "value" in field_name:
        # SIMPLIFY {"value":value} AS STRING
        field_name = field_name["value"]

    # SIMPLE PYTHON ITERABLE ASSUMED
    if isinstance(field_name, basestring):
        if len(split_field(field_name)) == 1:
            return [(d[field_name], ) for d in data]
        else:
            path = split_field(field_name)
            output = []
            flat_list._tuple1(data, path, 0, output)
            return output
    elif isinstance(field_name, list):
        paths = [_select_a_field(f) for f in field_name]
        output = DictList()
        _tuple((), unwrap(data), paths, 0, output)
        return output
    else:
        paths = [_select_a_field(field_name)]
        output = DictList()
        _tuple((), data, paths, 0, output)
        return output


def _tuple(template, data, fields, depth, output):
    deep_path = None
    deep_fields = DictList()
    for d in data:
        record = template
        for f in fields:
            index, children, record = _tuple_deep(d, f, depth, record)
            if index:
                path = f.value[0:index:]
                deep_fields.append(f)
                if deep_path and path != deep_path:
                    Log.error("Dangerous to select into more than one branch at time")
        if not children:
            output.append(record)
        else:
            _tuple(record, children, deep_fields, depth + 1, output)

    return output


def _tuple_deep(v, field, depth, record):
    """
    field = {"name":name, "value":["attribute", "path"]}
    r[field.name]=v[field.value], BUT WE MUST DEAL WITH POSSIBLE LIST IN field.value PATH
    """
    if hasattr(field.value, '__call__'):
        return 0, None, record + (field.value(v), )

    for i, f in enumerate(field.value[depth:len(field.value) - 1:]):
        v = v.get(f)
        if isinstance(v, list):
            return depth + i + 1, v, record

    f = field.value.last()
    return 0, None, record + (v.get(f), )


def select_one(record, selection):
    """
    APPLY THE selection TO A SINGLE record
    """
    record = wrap(record)
    selection = wrap(selection)

    if isinstance(selection, Mapping):
        selection = wrap(selection)
        return record[selection.value]
    elif isinstance(selection, basestring):
        return record[selection]
    elif isinstance(selection, list):
        output = Dict()
        for f in selection:
            f = _normalize_select(f)
            output[f.name]=record[f.value]
        return output
    else:
        Log.error("Do not know how to handle")



def select(data, field_name):
    """
    return list with values from field_name
    """
    if isinstance(data, Cube):
        return data._select(_normalize_selects(field_name))

    if isinstance(data, FlatList):
        return data.select(field_name)

    if isinstance(data, UniqueIndex):
        data = data._data.values()  # THE SELECT ROUTINE REQUIRES dicts, NOT Dict WHILE ITERATING

    if isinstance(data, Mapping):
        return select_one(data, field_name)

    if isinstance(field_name, Mapping):
        field_name = wrap(field_name)
        if field_name.value in ["*", "."]:
            return data

        if field_name.value:
            # SIMPLIFY {"value":value} AS STRING
            field_name = field_name.value

    # SIMPLE PYTHON ITERABLE ASSUMED
    if isinstance(field_name, basestring):
        path = split_field(field_name)
        if len(path) == 1:
            return DictList([d[field_name] for d in data])
        else:
            output = DictList()
            flat_list._select1(data, path, 0, output)
            return output
    elif isinstance(field_name, list):
        keys = [_select_a_field(wrap(f)) for f in field_name]
        return _select(Dict(), unwrap(data), keys, 0)
    else:
        keys = [_select_a_field(field_name)]
        return _select(Dict(), unwrap(data), keys, 0)


def _select_a_field(field):
    if isinstance(field, basestring):
        return wrap({"name": field, "value": split_field(field)})
    elif isinstance(wrap(field).value, basestring):
        field = wrap(field)
        return wrap({"name": field.name, "value": split_field(field.value)})
    else:
        return wrap({"name": field.name, "value": field.value})


def _select(template, data, fields, depth):
    output = DictList()
    deep_path = []
    deep_fields = UniqueIndex(["name"])
    for d in data:
        if isinstance(d, Dict):
            Log.error("programmer error, _select can not handle Dict")

        record = template.copy()
        children = None
        for f in fields:
            index, c = _select_deep(d, f, depth, record)
            children = c if children is None else children
            if index:
                path = f.value[0:index:]
                if not deep_fields[f]:
                    deep_fields.add(f)  # KEEP TRACK OF WHICH FIELDS NEED DEEPER SELECT
                short = MIN(len(deep_path), len(path))
                if path[:short:] != deep_path[:short:]:
                    Log.error("Dangerous to select into more than one branch at time")
                if len(deep_path) < len(path):
                    deep_path = path
        if not children:
            output.append(record)
        else:
            output.extend(_select(record, children, deep_fields, depth + 1))

    return output


def _select_deep(v, field, depth, record):
    """
    field = {"name":name, "value":["attribute", "path"]}
    r[field.name]=v[field.value], BUT WE MUST DEAL WITH POSSIBLE LIST IN field.value PATH
    """
    if hasattr(field.value, '__call__'):
        try:
            record[field.name] = field.value(wrap(v))
        except Exception, e:
            record[field.name] = None
        return 0, None

    for i, f in enumerate(field.value[depth:len(field.value) - 1:]):
        v = v.get(f)
        if v is None:
            return 0, None
        if isinstance(v, list):
            return depth + i + 1, v

    f = field.value.last()
    try:
        if not f:  # NO NAME FIELD INDICATES SELECT VALUE
            record[field.name] = v
        else:
            record[field.name] = v.get(f)
    except Exception, e:
        Log.error("{{value}} does not have {{field}} property",  value= v, field=f, cause=e)
    return 0, None


def _select_deep_meta(field, depth):
    """
    field = {"name":name, "value":["attribute", "path"]}
    r[field.name]=v[field.value], BUT WE MUST DEAL WITH POSSIBLE LIST IN field.value PATH
    RETURN FUNCTION THAT PERFORMS THE MAPPING
    """
    name = field.name
    if hasattr(field.value, '__call__'):
        try:
            def assign(source, destination):
                destination[name] = field.value(wrap(source))
                return 0, None
            return assign
        except Exception, e:
            def assign(source, destination):
                destination[name] = None
                return 0, None
            return assign

    prefix = field.value[depth:len(field.value) - 1:]
    if prefix:
        def assign(source, destination):
            for i, f in enumerate(prefix):
                source = source.get(f)
                if source is None:
                    return 0, None
                if isinstance(source, list):
                    return depth + i + 1, source

            f = field.value.last()
            try:
                if not f:  # NO NAME FIELD INDICATES SELECT VALUE
                    destination[name] = source
                else:
                    destination[name] = source.get(f)
            except Exception, e:
                Log.error("{{value}} does not have {{field}} property",  value= source, field=f, cause=e)
            return 0, None
        return assign
    else:
        f = field.value[0]
        if not f:  # NO NAME FIELD INDICATES SELECT VALUE
            def assign(source, destination):
                destination[name] = source
                return 0, None
            return assign
        else:
            def assign(source, destination):
                try:
                    destination[name] = source.get(f)
                except Exception, e:
                    Log.error("{{value}} does not have {{field}} property",  value= source, field=f, cause=e)
                return 0, None
            return assign


def get_columns(data):
    return wrap([{"name": n} for n in UNION(set(d.keys()) for d in data)])


def sort(data, fieldnames=None):
    """
    PASS A FIELD NAME, OR LIST OF FIELD NAMES, OR LIST OF STRUCTS WITH {"field":field_name, "sort":direction}
    """
    try:
        if data == None:
            return Null

        if fieldnames == None:
            return wrap(sorted(data))

        fieldnames = listwrap(fieldnames)
        if len(fieldnames) == 1:
            fieldnames = fieldnames[0]
            # SPECIAL CASE, ONLY ONE FIELD TO SORT BY
            if isinstance(fieldnames, (basestring, int)):
                fieldnames = wrap({"field": fieldnames, "sort": 1})

            # EXPECTING {"field":f, "sort":i} FORMAT
            fieldnames.sort = sort_direction.get(fieldnames.sort, 1)
            fieldnames.field = coalesce(fieldnames.field, fieldnames.value)
            if fieldnames.field==None:
                Log.error("Expecting sort to have 'field' attribute")

            if fieldnames.field == ".":
                #VALUE COMPARE
                def _compare_v(l, r):
                    return value_compare(l, r, fieldnames.sort)
                return DictList([unwrap(d) for d in sorted(data, cmp=_compare_v)])
            else:
                def _compare_o(left, right):
                    return value_compare(coalesce(left)[fieldnames.field], coalesce(right)[fieldnames.field], fieldnames.sort)
                return DictList([unwrap(d) for d in sorted(data, cmp=_compare_o)])

        formal = query._normalize_sort(fieldnames)

        def comparer(left, right):
            left = coalesce(left)
            right = coalesce(right)
            for f in formal:
                try:
                    result = value_compare(left[f.field], right[f.field], f.sort)
                    if result != 0:
                        return result
                except Exception, e:
                    Log.error("problem with compare", e)
            return 0

        if isinstance(data, list):
            output = DictList([unwrap(d) for d in sorted(data, cmp=comparer)])
        elif hasattr(data, "__iter__"):
            output = DictList([unwrap(d) for d in sorted(list(data), cmp=comparer)])
        else:
            Log.error("Do not know how to handle")
            output = None

        return output
    except Exception, e:
        Log.error("Problem sorting\n{{data}}",  data= data, cause=e)


def value_compare(l, r, ordering=1):
    if l == None:
        if r == None:
            return 0
        else:
            return - ordering
    elif r == None:
        return ordering
    else:
        return cmp(l, r) * ordering





def pairwise(values):
    """
    WITH values = [a, b, c, d, ...]
    RETURN [(a, b), (b, c), (c, d), ...]
    """
    i = iter(values)
    a = next(i)

    for b in i:
        yield (a, b)
        a = b



def filter(data, where):
    """
    where  - a function that accepts (record, rownum, rows) and returns boolean
    """
    if len(data) == 0 or where == None or where == TRUE_FILTER:
        return data

    if isinstance(data, Cube):
        data.filter(where)

    try:
        return drill_filter(where, data)
    except Exception, e:
        # WOW!  THIS IS INEFFICIENT!
        return wrap([unwrap(d) for d in drill_filter(where, [DictObject(d) for d in data])])


def drill_filter(esfilter, data):
    """
    PARTIAL EVALUATE THE FILTER BASED ON DATA GIVEN

    TODO:  FIX THIS MONUMENALLY BAD IDEA
    """
    esfilter = unwrap(esfilter)
    primary_nested = []  # track if nested, changes if not
    primary_column = []  # only one path allowed
    primary_branch = []  # CONTAINS LISTS OF RECORDS TO ITERATE: constantly changing as we dfs the tree

    def parse_field(fieldname, data, depth):
        """
        RETURN (first, rest) OF fieldname
        """
        col = split_field(fieldname)
        d = data
        for i, c in enumerate(col):
            d = d[c]
            if isinstance(d, list) and len(col) > 1:
                if len(primary_column) <= depth+i:
                    primary_nested.append(True)
                    primary_column.append(c)
                    primary_branch.append(d)
                elif primary_nested[depth] and primary_column[depth+i] != c:
                    Log.error("only one branch of tree allowed")
                else:
                    primary_nested[depth+i] = True
                    primary_column[depth+i] = c
                    primary_branch[depth+i] = d

                return c, join_field(col[i+1:])
            else:
                if len(primary_column) <= depth+i:
                    primary_nested.append(False)
                    primary_column.append(c)
                    primary_branch.append([d])
        return fieldname, None

    def pe_filter(filter, data, depth):
        """
        PARTIAL EVALUATE THE filter BASED ON data GIVEN
        """
        if filter is TRUE_FILTER:
            return True
        if filter is FALSE_FILTER:
            return False

        filter = wrap(filter)

        if filter["and"]:
            result = True
            output = DictList()
            for a in filter[u"and"]:
                f = pe_filter(a, data, depth)
                if f is False:
                    result = False
                elif f is not True:
                    output.append(f)
            if result and output:
                return {"and": output}
            else:
                return result
        elif filter["or"]:
            output = DictList()
            for o in filter[u"or"]:
                f = pe_filter(o, data, depth)
                if f is True:
                    return True
                elif f is not False:
                    output.append(f)
            if output:
                return {"or": output}
            else:
                return False
        elif filter["not"]:
            f = pe_filter(filter["not"], data, depth)
            if f is True:
                return False
            elif f is False:
                return True
            else:
                return {"not": f}
        elif filter.term:
            result = True
            output = {}
            for col, val in filter["term"].items():
                first, rest = parse_field(col, data, depth)
                d = data[first]
                if not rest:
                    if d != val:
                        result = False
                else:
                    output[rest] = val

            if result and output:
                return {"term": output}
            else:
                return result
        elif filter.equal:
            a, b = filter["equal"]
            first_a, rest_a = parse_field(a, data, depth)
            first_b, rest_b = parse_field(b, data, depth)
            val_a = data[first_a]
            val_b = data[first_b]
            if not rest_a:
                if not rest_b:
                    if val_a != val_b:
                        return False
                    else:
                        return True
                else:
                    return {"term": {rest_b: val_a}}
            else:
                if not rest_b:
                    return {"term": {rest_a: val_b}}
                else:
                    return {"equal": [rest_a, rest_b]}

        elif filter.terms:
            result = True
            output = {}
            for col, vals in filter["terms"].items():
                first, rest = parse_field(col, data, depth)
                d = data[first]
                if not rest:
                    if d not in vals:
                        result = False
                else:
                    output[rest] = vals
            if result and output:
                return {"terms": output}
            else:
                return result

        elif filter.range:
            result = True
            output = {}
            for col, ranges in filter["range"].items():
                first, rest = parse_field(col, data, depth)
                d = data[first]
                if not rest:
                    for sign, val in ranges.items():
                        if sign in ("gt", ">") and d <= val:
                            result = False
                        if sign == "gte" and d < val:
                            result = False
                        if sign == "lte" and d > val:
                            result = False
                        if sign == "lt" and d >= val:
                            result = False
                else:
                    output[rest] = ranges
            if result and output:
                return {"range": output}
            else:
                return result
        elif filter.missing:
            if isinstance(filter.missing, basestring):
                field = filter["missing"]
            else:
                field = filter["missing"]["field"]

            first, rest = parse_field(field, data, depth)
            d = data[first]
            if not rest:
                if d == None:
                    return True
                return False
            else:
                return {"missing": rest}
        elif filter.prefix:
            result = True
            output = {}
            for col, val in filter["prefix"].items():
                first, rest = parse_field(col, data, depth)
                d = data[first]
                if not rest:
                    if d==None or not d.startswith(val):
                        result = False
                else:
                    output[rest] = val
            if result and output:
                return {"prefix": output}
            else:
                return result

        elif filter.exists:
            if isinstance(filter["exists"], basestring):
                field = filter["exists"]
            else:
                field = filter["exists"]["field"]

            first, rest = parse_field(field, data, depth)
            d = data[first]
            if not rest:
                if d != None:
                    return True
                return False
            else:
                return {"exists": rest}
        else:
            Log.error(u"Can not interpret esfilter: {{esfilter}}", {u"esfilter": filter})

    output = []  # A LIST OF OBJECTS MAKING THROUGH THE FILTER

    def main(sequence, esfilter, row, depth):
        """
        RETURN A SEQUENCE OF REFERENCES OF OBJECTS DOWN THE TREE
        SHORT SEQUENCES MEANS ALL NESTED OBJECTS ARE INCLUDED
        """
        new_filter = pe_filter(esfilter, row, depth)
        if new_filter is True:
            seq = list(sequence)
            seq.append(row)
            output.append(seq)
            return
        elif new_filter is False:
            return

        seq = list(sequence)
        seq.append(row)
        for d in primary_branch[depth]:
            main(seq, new_filter, d, depth + 1)

    # OUTPUT
    for i, d in enumerate(data):
        if isinstance(d, Mapping):
            main([], esfilter, wrap(d), 0)
        else:
            Log.error("filter is expecting a dict, not {{type}}", type=d.__class__)

    # AT THIS POINT THE primary_column[] IS DETERMINED
    # USE IT TO EXPAND output TO ALL NESTED OBJECTS
    max = 0  # EVEN THOUGH A ROW CAN HAVE MANY VALUES, WE ONLY NEED UP TO max
    for i, n in enumerate(primary_nested):
        if n:
            max = i + 1

    # OUTPUT IS A LIST OF ROWS,
    # WHERE EACH ROW IS A LIST OF VALUES SEEN DURING A WALK DOWN A PATH IN THE HIERARCHY
    uniform_output = DictList()
    def recurse(row, depth):
        if depth == max:
            uniform_output.append(row)
        else:
            nested = row[-1][primary_column[depth]]
            if not nested:
                # PASSED FILTER, BUT NO CHILDREN, SO ADD NULL CHILDREN
                for i in range(depth, max):
                    row.append(None)
                uniform_output.append(row)
            else:
                for d in nested:
                    r = list(row)
                    r.append(d)
                    recurse(r, depth + 1)

    for o in output:
        recurse(o, 0)

    if not max:
        # SIMPLE LIST AS RESULT
        return wrap([unwrap(u[0]) for u in uniform_output])

    return FlatList(primary_column[0:max], uniform_output)


def wrap_function(func):
    """
    RETURN A THREE-PARAMETER WINDOW FUNCTION TO MATCH
    """
    if isinstance(func, basestring):
        return compile_expression(func)

    numarg = func.__code__.co_argcount
    if numarg == 0:
        def temp(row, rownum, rows):
            return func()

        return temp
    elif numarg == 1:
        def temp(row, rownum, rows):
            return func(row)

        return temp
    elif numarg == 2:
        def temp(row, rownum, rows):
            return func(row, rownum)

        return temp
    elif numarg == 3:
        return func


def window(data, param):
    """
    MAYBE WE CAN DO THIS WITH NUMPY (no, the edges of windows are not graceful with numpy)
    data - list of records
    """
    name = param.name            # column to assign window function result
    edges = param.edges          # columns to gourp by
    where = param.where          # DO NOT CONSIDER THESE VALUES
    sortColumns = param.sort            # columns to sort by
    calc_value = wrap_function(qb_expression_to_function(param.value)) # function that takes a record and returns a value (for aggregation)
    aggregate = param.aggregate  # WindowFunction to apply
    _range = param.range          # of form {"min":-10, "max":0} to specify the size and relative position of window

    data = filter(data, where)

    if not aggregate and not edges:
        if sortColumns:
            data = sort(data, sortColumns)
        # SIMPLE CALCULATED VALUE
        for rownum, r in enumerate(data):
            r[name] = calc_value(r, rownum, data)
        return

    if not aggregate or aggregate == "none":
        for _, values in groupby(data, edges.value):
            if not values:
                continue     # CAN DO NOTHING WITH THIS ZERO-SAMPLE

            sequence = sort(values, sortColumns)

            for rownum, r in enumerate(sequence):
                r[name] = calc_value(r, rownum, sequence)
        return

    for keys, values in groupby(data, edges.value):
        if not values:
            continue     # CAN DO NOTHING WITH THIS ZERO-SAMPLE

        sequence = sort(values, sortColumns)

        for rownum, r in enumerate(sequence):
            r["__temp__"] = calc_value(r, rownum, sequence)

        head = coalesce(_range.max, _range.stop)
        tail = coalesce(_range.min, _range.start)

        # PRELOAD total
        total = aggregate()
        for i in range(tail, head):
            total.add(sequence[i].__temp__)

        # WINDOW FUNCTION APPLICATION
        for i, r in enumerate(sequence):
            r[name] = total.end()
            total.add(sequence[i + head].__temp__)
            total.sub(sequence[i + tail].__temp__)

    for r in data:
        r["__temp__"] = None  # CLEANUP







def intervals(_min, _max=None, size=1):
    """
    RETURN (min, max) PAIRS OF GIVEN SIZE, WHICH COVER THE _min, _max RANGE
    THE LAST PAIR MAY BE SMALLER
    Yes!  It's just like range(), only cooler!
    """
    if _max == None:
        _max = _min
        _min = 0
    _max = int(Math.ceiling(_max))
    _min = int(Math.floor(_min))

    output = ((x, min(x + size, _max)) for x in __builtin__.range(_min, _max, size))
    return output


def accumulate(vals):
    """
    RETURN PAIRS IN FORM (sum(vals[0:i-1]), vals[i])
    THE FIRST IN TUPLE IS THE SUM OF ALL VALUE BEFORE
    """
    sum = 0
    for v in vals:
        yield sum, v
        sum += v

def reverse(vals):
    # TODO: Test how to do this fastest
    l = len(vals)
    output = [None] * l

    for v in unwrap(vals):
        l -= 1
        output[l] = v

    return wrap(output)

from pyLibrary.queries.list.aggs import is_aggs, list_aggs
