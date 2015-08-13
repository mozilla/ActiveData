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
from copy import copy

from pyLibrary.debugs.logs import Log
from pyLibrary.dot import set_default, wrap, coalesce, Dict, listwrap, unwraplist
from pyLibrary.maths import Math
from pyLibrary.queries.dimensions import Dimension
from pyLibrary.queries.domains import is_keyword
from pyLibrary.queries.namespace import Namespace, convert_list
from pyLibrary.queries.query import Query
from pyLibrary.times.dates import Date


class Rename(Namespace):

    def __init__(self, dimensions):
        """
        EXPECTING A LIST OF {"name":name, "value":value} OBJECTS TO PERFORM A MAPPING
        """
        self.converter_map = {
            "and": self._convert_many,
            "or": self._convert_many,
            "not": self.convert,
            "missing": self.convert,
            "exists": self.convert
        }
        self.dimensions = Dict()
        for d in listwrap(dimensions):
            self.addDimension(d)

    def addDimension(self, dim):
        if isinstance(dim, list):
            Log.error("Expecting dimension to be a object, not a list:\n{{dim}}",  dim= dim)
        self._addDimension(dim, [])

    def _addDimension(self, dim, path):
        dim.full_name = dim.name
        for e in dim.edges:
            d = Dimension(e, dim, self)
            self.dimensions[d.full_name] = d



    def convert(self, expr):
        """
        EXPAND INSTANCES OF name TO value
        """
        if expr is True or expr == None or expr is False:
            return expr
        elif Math.is_number(expr):
            return expr
        elif expr == ".":
            return "."
        elif is_keyword(expr):
            return coalesce(self.dimensions[expr], expr)
        elif isinstance(expr, basestring):
            Log.error("{{name|quote}} is not a valid variable name", name=expr)
        elif isinstance(expr, Date):
            return expr
        elif isinstance(expr, Query):
            return self._convert_query(expr)
        elif isinstance(expr, Mapping):
            if expr["from"]:
                return self._convert_query(expr)
            elif len(expr) >= 2:
                #ASSUME WE HAVE A NAMED STRUCTURE, NOT AN EXPRESSION
                return wrap({name: self.convert(value) for name, value in expr.leaves()})
            else:
                # ASSUME SINGLE-CLAUSE EXPRESSION
                k, v = expr.items()[0]
                return self.converter_map.get(k, self._convert_bop)(k, v)
        elif isinstance(expr, (list, set, tuple)):
            return wrap([self.convert(value) for value in expr])

    def _convert_query(self, query):
        output = Query()
        output.select = self._convert_clause(query.select)
        output.where = self.convert(query.where)
        output["from"] = self._convert_from(query["from"])
        output.edges = convert_list(self._convert_edge, query.edges)
        output.having = convert_list(self._convert_having, query.having)
        output.window = convert_list(self._convert_window, query.window)
        output.sort = self._convert_clause(query.sort)
        output.format = query.format

        return output




    def _convert_bop(self, op, term):
        if isinstance(term, list):
            return {op: map(self.convert, term)}

        return {op: {self.convert(var): val for var, val in term.items()}}

    def _convert_many(self, k, v):
        return {k: map(self.convert, v)}

    def _convert_from(self, frum):
        if isinstance(frum, Mapping):
            return Dict(name=self.convert(frum.name))
        else:
            return self.convert(frum)

    def _convert_edge(self, edge):
        dim = self.dimensions[edge.value]
        if not dim:
            return edge

        if len(listwrap(dim.domain.fields)) == 1:
            #TODO: CHECK IF EDGE DOMAIN AND DIMENSION DOMAIN CONFILICT
            edge.value = unwraplist(dim.domain.fields)
        else:
            edge = copy(edge)
            edge.value = None
            edge.domain = dim.get_domain()

    def _convert_clause(self, clause):
        """
        Qb QUERIES HAVE MANY CLAUSES WITH SIMILAR COLUMN DELCARATIONS
        """
        clause = wrap(clause)

        if clause == None:
            return None
        elif isinstance(clause, Mapping):
            return set_default({"value": self.convert(clause.value)}, clause)
        else:
            return [set_default({"value": self.convert(c.value)}, c) for c in clause]

