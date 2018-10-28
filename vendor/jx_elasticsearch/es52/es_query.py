# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import Mapping

from mo_dots import startswith_field
from mo_future import text_type
from mo_json import value2json
from mo_logs import Log


class Aggs(object):

    def __init__(self, name=None):
        self.name = name
        self.children = []

    def to_es(self, schema, query_path="."):
        if self.children:
            return {"aggs": {
                name: t.to_es(schema, query_path)
                for i, t in enumerate(self.children)
                for name in [t.name if t.name else "_" + text_type(i)]
            }}
        else:
            return {}

    def add(self, child):
        self.children.append(child)
        return self

    def __eq__(self, other):
        return self is other

    def __str__(self):
        return value2json(self.to_es)


class ExprAggs(Aggs):

    def __init__(self, name, expr):
        Aggs.__init__(self, name)
        self.name = name
        self.expr = expr

    def to_es(self, schema, query_path="."):
        self.expr['aggs']=Aggs.to_es(self, schema, query_path).get('aggs')
        return self.expr


class FilterAggs(Aggs):
    def __init__(self, name, filter):
        Aggs.__init__(self, name)
        self.filter = filter
        if isinstance(filter, Mapping):
            Log.error("programming error")

    def to_es(self, schema, query_path="."):
        output = Aggs.to_es(self, schema, query_path)
        output['filter'] = self.filter.partial_eval().to_esfilter(schema)
        return output


class FiltersAggs(Aggs):
    def __init__(self, name, filters):
        Aggs.__init__(self, name)
        self.filters = filters
        if not isinstance(filters, list):
            Log.error("expecting a list")

    def to_es(self, schema, query_path="."):
        output = Aggs.to_es(self, schema, query_path)
        output['filters'] = {"filters": [f.partial_eval().to_esfilter(schema) for f in self.filters]}
        return output


class NestedAggs(Aggs):
    def __init__(self, path):
        Aggs.__init__(self, "_nested")
        self.path = path

    def to_es(self, schema, query_path="."):
        output = Aggs.to_es(self, schema, query_path)
        if query_path == self.path:
            Log.error("this should have been cancelled out")
        elif startswith_field(self.path, query_path):
            output['nested'] = {"path": self.path}
        else:
            output["reverse_nested"] = {"path": None if self.path == "." else self.path}
        return output

    def __eq__(self, other):
        return isinstance(other, NestedAggs) and self.path == other.path


class TermsAggs(Aggs):
    def __init__(self, name, terms):
        Aggs.__init__(self, name)
        self.terms = terms

    def to_es(self, schema, query_path="."):
        output = Aggs.to_es(self, schema, query_path)
        output['terms'] = self.terms
        return output


class RangeAggs(Aggs):
    def __init__(self, name, expr):
        Aggs.__init__(self, name)
        self.expr = expr

    def to_es(self, schema, query_path="."):
        output = Aggs.to_es(self, schema, query_path)
        output['range'] = self.expr
        return output


def simplify(aggs):
    # CONVERT FROM TREE TO UNION OF SEQUENCES
    def depth_first(aggr):
        if aggr.__class__ == Aggs:
            # BASE CLASS Aggs IS ONLY A PLACEHOLDER
            if not aggr.children:
                yield tuple()
                return
            for c in aggr.children:
                for path in depth_first(c):
                    yield path
        elif not aggr.children:
            yield (aggr,)
        else:
            for c in aggr.children:
                for path in depth_first(c):
                    yield (aggr,) + path

    # CANCEL OUT REDUNDANT NESTED AGGS
    combined = []
    for path in depth_first(aggs):
        current_nested = NestedAggs(".")
        prev = None
        remove = []
        for step in path:
            if isinstance(step, NestedAggs):
                if prev is not None:
                    remove.append(prev)
                    prev = None
                if current_nested is not None:
                    if current_nested.path == step.path:
                        remove.append(step)
                        continue
                    else:
                        pass
                prev = step
            else:
                current_nested = prev if prev else current_nested
                prev = None

        combined.append(tuple(p for p in path if not any(p is r for r in remove)))

    # COMMON FACTOR, CONVERT BACK TO TREE
    def merge(terms):
        output = []
        while True:
            common = []
            f = None
            for i, t in enumerate(terms):
                if not t:
                    continue
                if f is None:
                    f = t[0]
                if t[0] == f:
                    common.append(t[1:])
                    terms[i] = None

            if f is None:
                return output
            else:
                f.children = merge(common)
            output.append(f)

    merged = [trim_root(o) for o in merge(combined)]

    output = Aggs()
    output.children = merged
    return output


def trim_root(agg):
    if isinstance(agg, NestedAggs) and agg.path == '.':
        if len(agg.children) == 1:
            return agg.children[0]
        else:
            output = Aggs()
            output.children = agg.children
            return output
    else:
        return agg
