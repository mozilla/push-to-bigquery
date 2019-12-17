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

from jx_base.expressions import FALSE, TRUE, WhenOp as WhenOp_
from jx_elasticsearch.es52.painless._utils import Painless
from jx_elasticsearch.es52.painless.es_script import EsScript
from mo_json import INTEGER, NUMBER
from mo_logs import Log


class WhenOp(WhenOp_):
    def to_es_script(self, schema, not_null=False, boolean=False, many=True):
        if self.simplified:
            when = Painless[self.when].to_es_script(schema)
            then = Painless[self.then].to_es_script(schema)
            els_ = Painless[self.els_].to_es_script(schema)

            if when is TRUE:
                return then
            elif when is FALSE:
                return els_
            elif then.miss is TRUE:
                return EsScript(
                    miss=self.missing(),
                    type=els_.type,
                    expr=els_.expr,
                    frum=self,
                    schema=schema,
                )
            elif els_.miss is TRUE:
                return EsScript(
                    miss=self.missing(),
                    type=then.type,
                    expr=then.expr,
                    frum=self,
                    schema=schema,
                )

            elif then.type == els_.type:
                return EsScript(
                    miss=self.missing(),
                    type=then.type,
                    expr="("
                    + when.expr
                    + ") ? ("
                    + then.expr
                    + ") : ("
                    + els_.expr
                    + ")",
                    frum=self,
                    schema=schema,
                )
            elif then.type in (INTEGER, NUMBER) and els_.type in (INTEGER, NUMBER):
                return EsScript(
                    miss=self.missing(),
                    type=NUMBER,
                    expr="("
                    + when.expr
                    + ") ? ("
                    + then.expr
                    + ") : ("
                    + els_.expr
                    + ")",
                    frum=self,
                    schema=schema,
                )
            else:
                Log.error("do not know how to handle: {{self}}", self=self.__data__())
        else:
            return self.partial_eval().to_es_script(schema)
