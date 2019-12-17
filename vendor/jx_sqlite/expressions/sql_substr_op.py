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

from jx_base.expressions import NULL, SqlSubstrOp as SqlSubstrOp_
from jx_sqlite.expressions._utils import check
from jx_sqlite.expressions.literal import Literal
from mo_dots import wrap
from pyLibrary.sql import sql_iso, sql_list


class SqlSubstrOp(SqlSubstrOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        value = self.value.to_sql(schema, not_null=True)[0].sql.s
        start = self.start.to_sql(schema, not_null=True)[0].sql.n
        if self.length is NULL:
            sql = "SUBSTR" + sql_iso(sql_list([value, start]))
        else:
            length = self.length.to_sql(schema, not_null=True)[0].sql.n
            sql = "SUBSTR" + sql_iso(sql_list([value, start, length]))
        return wrap([{"name": ".", "sql": sql}])

    def partial_eval(self):
        value = self.value.partial_eval()
        start = self.start.partial_eval()
        length = self.length.partial_eval()
        if isinstance(start, Literal) and start.value == 1:
            if length is NULL:
                return value
        return SqlSubstrOp([value, start, length])
