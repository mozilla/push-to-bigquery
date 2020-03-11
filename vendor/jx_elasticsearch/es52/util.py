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
from jx_base.expressions import Variable
from jx_base.language import is_op
from jx_base.query import DEFAULT_LIMIT, MAX_LIMIT
from jx_elasticsearch.es52.expressions.and_op import es_and
from mo_dots import wrap, Null, coalesce
from mo_future import is_text, first
from mo_json import BOOLEAN, IS_NULL, NUMBER, OBJECT, STRING, NUMBER_TYPES
from mo_logs import Log
from pyLibrary.convert import value2boolean


def es_query_template(path):
    """
    RETURN TEMPLATE AND PATH-TO-FILTER AS A 2-TUPLE
    :param path: THE NESTED PATH (NOT INCLUDING TABLE NAME)
    :return: (es_query, es_filters) TUPLE
    """

    if not is_text(path):
        Log.error("expecting path to be a string")

    if path != ".":
        f0 = {}
        f1 = {}
        output = wrap({
            "query": es_and([
                f0,
                {"nested": {
                    "path": path,
                    "query": f1,
                    "inner_hits": {"size": 100000}
                }}
            ]),
            "from": 0,
            "size": 0,
            "sort": []
        })
        return output, wrap([f0, f1])
    else:
        f0 = {}
        output = wrap({
            "query": es_and([f0]),
            "from": 0,
            "size": 0,
            "sort": []
        })
        return output, wrap([f0])


def jx_sort_to_es_sort(sort, schema):
    if not sort:
        return []

    output = []
    for s in sort:
        if is_op(s.value, Variable):
            cols = schema.leaves(s.value.var)
            if s.sort == -1:
                types = OBJECT, STRING, NUMBER, BOOLEAN
            else:
                types = BOOLEAN, NUMBER, STRING, OBJECT

            for type in types:
                for c in cols:
                    if c.jx_type == type or (c.jx_type in NUMBER_TYPES and type in NUMBER_TYPES):
                        np = first(c.nested_path)
                        if np == '.':
                            if s.sort == -1:
                                output.append({c.es_column: "desc"})
                            else:
                                output.append(c.es_column)
                        else:
                            output.append({c.es_column: {
                                "order": {1: "asc", -1: "desc"}[s.sort],
                                "nested": {
                                    "path": np,
                                    "filter": {"match_all": {}}
                                },
                            }})
        else:
            from mo_logs import Log

            Log.error("do not know how to handle")
    return output


# FOR ELASTICSEARCH aggs
aggregates = {
    "none": "none",
    "one": "count",
    "cardinality": "cardinality",
    "sum": "sum",
    "add": "sum",
    "count": "value_count",
    "count_values": "count_values",
    "maximum": "max",
    "minimum": "min",
    "and": "min",
    "or": "max",
    "max": "max",
    "min": "min",
    "mean": "avg",
    "average": "avg",
    "avg": "avg",
    "median": "median",
    "percentile": "percentile",
    "N": "count",
    "s0": "count",
    "s1": "sum",
    "s2": "sum_of_squares",
    "std": "std_deviation",
    "stddev": "std_deviation",
    "union": "union",
    "var": "variance",
    "variance": "variance",
    "stats": "stats"
}

NON_STATISTICAL_AGGS = {"none", "one"}


pull_functions = {
    IS_NULL: lambda x: None,
    STRING: lambda x: x,
    NUMBER: lambda x: float(x) if x !=None else None,
    BOOLEAN: value2boolean,
}


def temper_limit(proposed_limit, query):
    """
    SUITABLE DEFAULTS AND LIMITS
    """
    from jx_elasticsearch.es52.agg_bulk import is_bulk_agg
    from jx_elasticsearch.es52.set_bulk import is_bulk_set
    if is_bulk_agg(Null, query) or is_bulk_set(Null, query):
        return coalesce(proposed_limit, query.limit)
    else:
        return mo_math.min(coalesce(proposed_limit, query.limit, DEFAULT_LIMIT), MAX_LIMIT)


