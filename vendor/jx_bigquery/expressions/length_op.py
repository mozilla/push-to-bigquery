# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from pyLibrary.sql.sqlite import quote_value

from jx_base.expressions import LengthOp as LengthOp_, is_literal
from jx_bigquery.expressions._utils import SQLang, check
from mo_dots import Null, wrap
from mo_future import text
from pyLibrary import convert
from pyLibrary.sql import SQL, sql_iso, ConcatSQL


class LengthOp(LengthOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        term = SQLang[self.term].partial_eval()
        if is_literal(term):
            val = term.value
            if isinstance(val, text):
                sql = quote_value(len(val))
            elif isinstance(val, (float, int)):
                sql = quote_value(len(convert.value2json(val)))
            else:
                return Null
        else:
            value = term.to_sql(schema, not_null=not_null)[0].sql.s
            sql = ConcatSQL((SQL("LENGTH"), sql_iso(value)))
        return wrap([{"name": ".", "sql": {"n": sql}}])
