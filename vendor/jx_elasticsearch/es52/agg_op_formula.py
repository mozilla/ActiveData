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
from jx_base.expressions import NULL, TupleOp
from jx_base.language import is_op
from jx_elasticsearch.es52.es_query import ExprAggs, NestedAggs, TermsAggs
from jx_elasticsearch.es52.expressions import split_expression_by_path
from jx_elasticsearch.es52.painless import NumberOp, Painless
from jx_elasticsearch.es52.set_op import get_pull_stats
from jx_elasticsearch.es52.util import aggregates
from jx_python.expressions import jx_expression_to_function
from mo_dots import join_field, literal_field
from mo_future import first, text
from mo_json import BOOLEAN
from mo_logs import Log
from mo_logs.strings import expand_template

COMPARE_TUPLE = """
(a, b)->{
    int i=0;
    for (dummy in a){  //ONLY THIS FOR LOOP IS ACCEPTED (ALL OTHER FORMS THROW NullPointerException)
        if (a[i]==null){
            if (b[i]==null){
                return 0; 
            }else{
                return -1*({{dir}});
            }//endif
        }else if (b[i]==null) return {{dir}};

        if (a[i]!=b[i]) {
            if (a[i] instanceof Boolean){
                if (b[i] instanceof Boolean){
                    int cmp = Boolean.compare(a[i], b[i]);
                    if (cmp != 0) return cmp;
                } else {
                    return -1;
                }//endif                    
            }else if (a[i] instanceof Number) {
                if (b[i] instanceof Boolean) {
                    return 1                
                } else if (b[i] instanceof Number) {
                    int cmp = Double.compare(a[i], b[i]);
                    if (cmp != 0) return cmp;
                } else {
                    return -1;
                }//endif
            }else {
                if (b[i] instanceof Boolean) {
                    return 1;
                } else if (b[i] instanceof Number) {
                    return 1;
                } else {
                    int cmp = ((String)a[i]).compareTo((String)b[i]);
                    if (cmp != 0) return cmp;
                }//endif
            }//endif
        }//endif
        i=i+1;
    }//for
    return 0;
}
"""


MAX_OF_TUPLE = """
(Object[])([{{expr1}}, {{expr2}}].stream().{{op}}("""+COMPARE_TUPLE+""").get())
"""


def agg_formula(acc, formula, query_path, schema):
    # DUPLICATED FOR SCRIPTS, MAYBE THIS CAN BE PUT INTO A LANGUAGE?
    for i, s in enumerate(formula):
        canonical_name = s.name
        s_path = [k for k, v in split_expression_by_path(s.value, schema=schema, lang=Painless).items() if v]
        if len(s_path) == 0:
            # FOR CONSTANTS
            nest = NestedAggs(query_path)
            acc.add(nest)
        elif len(s_path) == 1:
            nest = NestedAggs(first(s_path))
            acc.add(nest)
        else:
            raise Log.error("do not know how to handle")

        if is_op(s.value, TupleOp):
            if s.aggregate == "count":
                # TUPLES ALWAYS EXIST, SO COUNTING THEM IS EASY
                s.pull = jx_expression_to_function("doc_count")
            elif s.aggregate in ('max', 'maximum', 'min', 'minimum'):
                if s.aggregate in ('max', 'maximum'):
                    dir = 1
                    op = "max"
                else:
                    dir = -1
                    op = 'min'

                nully = Painless[TupleOp([NULL] * len(s.value.terms))].partial_eval().to_es_script(schema)
                selfy = text(Painless[s.value].partial_eval().to_es_script(schema))

                script = {"scripted_metric": {
                    'init_script': 'params._agg.best = ' + nully + '.toArray();',
                    'map_script': 'params._agg.best = ' + expand_template(
                        MAX_OF_TUPLE,
                        {"expr1": "params._agg.best", "expr2": selfy,
                         "dir": dir, "op": op}
                    ) + ";",
                    'combine_script': 'return params._agg.best',
                    'reduce_script': 'return params._aggs.stream().' + op + '(' + expand_template(
                        COMPARE_TUPLE,
                        {"dir": dir,
                         "op": op}
                    ) + ').get()',
                }}
                nest.add(NestedAggs(query_path).add(
                    ExprAggs(canonical_name, script, s)
                ))
                s.pull = jx_expression_to_function("value")
            else:
                Log.error("{{agg}} is not a supported aggregate over a tuple", agg=s.aggregate)
        elif s.aggregate == "count":
            nest.add(ExprAggs(canonical_name,
                              {"value_count": {"script": text(Painless[s.value].partial_eval().to_es_script(schema))}},
                              s))
            s.pull = jx_expression_to_function("value")
        elif s.aggregate == "median":
            # ES USES DIFFERENT METHOD FOR PERCENTILES THAN FOR STATS AND COUNT
            key = literal_field(canonical_name + " percentile")
            nest.add(ExprAggs(key, {"percentiles": {
                "script": text(Painless[s.value].to_es_script(schema)),
                "percents": [50]
            }}, s))
            s.pull = jx_expression_to_function(join_field(["50.0"]))
        elif s.aggregate in ("and", "or"):
            key = literal_field(canonical_name + " " + s.aggregate)
            op = aggregates[s.aggregate]
            nest.add(
                ExprAggs(key, {op: {
                    "script": text(Painless[NumberOp(s.value)].to_es_script(schema))
                }}, s)
            )
            # get_name = concat_field(canonical_name, "value")
            s.pull = jx_expression_to_function({"case": [
                {"when": {"eq": {"value": 1}}, "then": True},
                {"when": {"eq": {"value": 0}}, "then": False}
            ]})
        elif s.aggregate == "percentile":
            # ES USES DIFFERENT METHOD FOR PERCENTILES THAN FOR STATS AND COUNT
            key = literal_field(canonical_name + " percentile")
            percent = mo_math.round(s.percentile * 100, decimal=6)
            nest.add(ExprAggs(key, {"percentiles": {
                "script": text(Painless[s.value].to_es_script(schema)),
                "percents": [percent]
            }}, s))
            s.pull = jx_expression_to_function(join_field(["values", text(percent)]))
        elif s.aggregate == "cardinality":
            # ES USES DIFFERENT METHOD FOR CARDINALITY
            key = canonical_name + " cardinality"
            nest.add(ExprAggs(key, {"cardinality": {"script": text(Painless[s.value].to_es_script(schema))}}, s))
            s.pull = jx_expression_to_function("value")
        elif s.aggregate == "stats":
            # REGULAR STATS
            nest.add(ExprAggs(canonical_name, {"extended_stats": {
                "script": text(Painless[s.value].to_es_script(schema))
            }}, s))
            s.pull = get_pull_stats()

            # GET MEDIAN TOO!
            select_median = s.copy()
            select_median.pull = jx_expression_to_function({"select": [{"name": "median", "value": "values.50\\.0"}]})

            nest.add(ExprAggs(canonical_name + "_percentile", {"percentiles": {
                "script": text(Painless[s.value].to_es_script(schema)),
                "percents": [50]
            }}, select_median))
            s.pull = get_pull_stats()
        elif s.aggregate == "union":
            # USE TERMS AGGREGATE TO SIMULATE union
            nest.add(TermsAggs(canonical_name, {"script_field": text(Painless[s.value].to_es_script(schema))}, s))
            s.pull = jx_expression_to_function("key")
        else:
            # PULL VALUE OUT OF THE stats AGGREGATE
            s.pull = jx_expression_to_function(aggregates[s.aggregate])
            nest.add(ExprAggs(canonical_name, {
                "extended_stats": {"script": text(NumberOp(s.value).partial_eval().to_es_script(schema))}}, s))

