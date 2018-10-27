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

from mo_dots import set_default
from mo_future import text_type
from mo_json import value2json


class Aggs(object):

    def __init__(self, name=None):
        self.name = name
        self.children = []

    def to_es(self, schema):
        return {
            name: t.to_es(schema)
            for i, t in enumerate(self.children)
            for name in [t.name if t.name else "_"+text_type(i)]
        }

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

    def to_es(self, schema):
        if self.children:
            return set_default(
                {"aggs": Aggs.to_es(self, schema)},
                self.expr
            )
        else:
            return self.expr


class FilterAggs(Aggs):
    def __init__(self, name, filter):
        Aggs.__init__(self, name)
        self.filter = filter

    def to_es(self, schema):
        return {
            "filter": self.filter.partial_eval().to_esfilter(schema),
            "aggs": Aggs.to_es(self, schema)
        }


class NestedAggs(Aggs):
    def __init__(self, path):
        Aggs.__init__(self, "_nested")
        self.path = path

    def to_es(self, schema):
        return {
            "nested": {"path": self.path},
            "aggs": Aggs.to_es(self, schema)
        }

    def __eq__(self, other):
        return isinstance(other, NestedAggs) and self.path == other.path


class TermsAggs(Aggs):
    def __init__(self, name, terms):
        Aggs.__init__(self, name)
        self.terms = terms

    def to_es(self, schema):
        return {
            "terms": self.terms,
            "aggs": Aggs.to_es(self, schema)
        }


class RangeAggs(Aggs):
    def __init__(self, expr):
        Aggs.__init__(self, None)
        self.expr = expr

    def to_es(self, schema):
        return {
            "range": self.expr
        }



def simplify(aggs):
    # CONVERT FROM TREE TO UNION OF SEQUENCES
    def depth_first(_aggs):
        if not _aggs.children:
            yield (_aggs,)
        elif _aggs.__class__ == Aggs:
            for c in _aggs.children:
                for path in depth_first(c):
                    yield path
        else:
            for c in _aggs.children:
                for path in depth_first(c):
                    yield (_aggs,) + path

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

    if len(merged) == 1:
        return merged[0]
    else:
        temp = Aggs()
        temp.children = merged
        return temp


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
