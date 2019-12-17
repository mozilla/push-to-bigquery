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

from jx_base.expressions import EsNestedOp as EsNestedOp_


class EsNestedOp(EsNestedOp_):
    def to_esfilter(self, schema):
        if self.path.var == ".":
            return {"query": self.query.to_esfilter(schema)}
        else:
            return {
                "nested": {
                    "path": self.path.var,
                    "query": self.query.to_esfilter(schema),
                }
            }
