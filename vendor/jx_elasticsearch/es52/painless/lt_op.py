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

from jx_base.expressions import LtOp as LtOp_
from jx_elasticsearch.es52.painless._utils import _inequality_to_es_script


class LtOp(LtOp_):
    to_es_script = _inequality_to_es_script
