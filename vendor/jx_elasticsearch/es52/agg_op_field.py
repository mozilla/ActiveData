# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

import mo_math
from jx_base.expressions import NULL
from jx_elasticsearch.es52.es_query import CountAggs, ExprAggs, NestedAggs
from jx_elasticsearch.es52.set_op import get_pull_stats
from jx_elasticsearch.es52.util import aggregates
from jx_python.expressions import jx_expression_to_function
from mo_dots import join_field
from mo_future import first, is_text, text
from mo_json import EXISTS, NESTED, OBJECT, NUMBER_TYPES, BOOLEAN
from mo_json.typed_encoder import encode_property
from mo_logs import Log
from mo_logs.strings import quote


def agg_field(acc, new_select, query_path, schema):
    for s in (s for _, many in new_select.items() for s in many):
        canonical_name = s.name
        if s.aggregate in ("value_count", "count"):
            columns = schema.values(s.value.var, exclude_type=(OBJECT, NESTED))
        else:
            columns = schema.values(s.value.var)

        if s.aggregate == "count":
            canonical_names = []
            for column in columns:
                es_name = column.es_column + "_count"
                if column.jx_type == EXISTS:
                    if column.nested_path[0] == query_path:
                        canonical_names.append("doc_count")
                        acc.add(NestedAggs(column.nested_path[0]).add(
                            CountAggs(s)
                        ))
                else:
                    canonical_names.append("value")
                    acc.add(NestedAggs(column.nested_path[0]).add(
                        ExprAggs(es_name, {"value_count": {"field": column.es_column}}, s)
                    ))
            if len(canonical_names) == 1:
                s.pull = jx_expression_to_function(canonical_names[0])
            else:
                s.pull = jx_expression_to_function({"add": canonical_names})
        elif s.aggregate == "median":
            columns = [c for c in columns if c.jx_type in NUMBER_TYPES]
            if len(columns) != 1:
                Log.error("Do not know how to perform median on columns with more than one type (script probably)")
            # ES USES DIFFERENT METHOD FOR PERCENTILES
            key = canonical_name + " percentile"
            acc.add(ExprAggs(key, {"percentiles": {
                "field": first(columns).es_column,
                "percents": [50]
            }}, s))
            s.pull = jx_expression_to_function("values.50\\.0")
        elif s.aggregate in ("and", "or"):
            columns = [c for c in columns if c.jx_type is BOOLEAN]
            op = aggregates[s.aggregate]
            if not columns:
                s.pull = jx_expression_to_function(NULL)
            else:
                for c in columns:
                    acc.add(NestedAggs(c.nested_path[0]).add(
                        ExprAggs(canonical_name, {op: {"field": c.es_column}}, s)
                    ))
                # get_name = concat_field(canonical_name, "value")
                s.pull = jx_expression_to_function({"case": [
                    {"when": {"eq": {"value": 1}}, "then": True},
                    {"when": {"eq": {"value": 0}}, "then": False}
                ]})
        elif s.aggregate == "percentile":
            columns = [c for c in columns if c.jx_type in NUMBER_TYPES]
            if len(columns) != 1:
                Log.error(
                    "Do not know how to perform percentile on columns with more than one type (script probably)")
            # ES USES DIFFERENT METHOD FOR PERCENTILES
            key = canonical_name + " percentile"
            if is_text(s.percentile) or s.percetile < 0 or 1 < s.percentile:
                Log.error("Expecting percentile to be a float from 0.0 to 1.0")
            percent = mo_math.round(s.percentile * 100, decimal=6)

            acc.add(ExprAggs(key, {"percentiles": {
                "field": first(columns).es_column,
                "percents": [percent],
                "tdigest": {"compression": 2}
            }}, s))
            s.pull = jx_expression_to_function(join_field(["values", text(percent)]))
        elif s.aggregate == "cardinality":
            for column in columns:
                path = column.es_column + "_cardinality"
                acc.add(ExprAggs(path, {"cardinality": {"field": column.es_column}}, s))
            s.pull = jx_expression_to_function("value")
        elif s.aggregate == "stats":
            columns = [c for c in columns if c.jx_type in NUMBER_TYPES]
            if len(columns) != 1:
                Log.error("Do not know how to perform stats on columns with more than one type (script probably)")
            # REGULAR STATS
            acc.add(ExprAggs(canonical_name, {"extended_stats": {
                "field": first(columns).es_column
            }}, s))
            s.pull = get_pull_stats()

            # GET MEDIAN TOO!
            select_median = s.copy()
            select_median.pull = jx_expression_to_function(
                {"select": [{"name": "median", "value": "values.50\\.0"}]})

            acc.add(ExprAggs(canonical_name + "_percentile", {"percentiles": {
                "field": first(columns).es_column,
                "percents": [50]
            }}, select_median))

        elif s.aggregate == "union":
            for column in columns:
                script = {"scripted_metric": {
                    'init_script': 'params._agg.terms = new HashSet()',
                    'map_script': 'for (v in doc[' + quote(
                        column.es_column) + '].values) params._agg.terms.add(v);',
                    'combine_script': 'return params._agg.terms.toArray()',
                    'reduce_script': 'HashSet output = new HashSet(); for (a in params._aggs) { if (a!=null) for (v in a) {output.add(v)} } return output.toArray()',
                }}
                stats_name = column.es_column
                acc.add(NestedAggs(column.nested_path[0]).add(ExprAggs(stats_name, script, s)))
            s.pull = jx_expression_to_function("value")
        elif s.aggregate == "count_values":
            # RETURN MAP FROM VALUE TO THE NUMBER OF TIMES FOUND IN THE DOCUMENTS
            # NOT A NESTED DOC, RATHER A MULTIVALUE FIELD
            for column in columns:
                script = {"scripted_metric": {
                    'params': {"_agg": {}},
                    'init_script': 'params._agg.terms = new HashMap()',
                    'map_script': 'for (v in doc[' + quote(
                        column.es_column) + '].values) params._agg.terms.put(v, Optional.ofNullable(params._agg.terms.get(v)).orElse(0)+1);',
                    'combine_script': 'return params._agg.terms',
                    'reduce_script': '''
                        HashMap output = new HashMap(); 
                        for (agg in params._aggs) {
                            if (agg!=null){
                                for (e in agg.entrySet()) {
                                    String key = String.valueOf(e.getKey());
                                    output.put(key, e.getValue() + Optional.ofNullable(output.get(key)).orElse(0));
                                } 
                            }
                        } 
                        return output;
                    '''
                }}
                stats_name = encode_property(column.es_column)
                acc.add(NestedAggs(column.nested_path[0]).add(ExprAggs(stats_name, script, s)))
            s.pull = jx_expression_to_function("value")
        else:
            if not columns:
                s.pull = jx_expression_to_function(NULL)
            else:
                for c in columns:
                    acc.add(NestedAggs(c.nested_path[0]).add(
                        ExprAggs(canonical_name, {"extended_stats": {"field": c.es_column}}, s)
                    ))
                s.pull = jx_expression_to_function(aggregates[s.aggregate])
