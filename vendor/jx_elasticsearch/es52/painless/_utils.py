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

from jx_base.language import Language

from jx_base.expressions import (
    FALSE,
    FalseOp as FalseOp_,
    NULL,
    NullOp,
    TRUE,
    TrueOp as TrueOp_,
    extend,
)
from jx_elasticsearch.es52.painless.and_op import AndOp
from jx_elasticsearch.es52.painless.es_script import EsScript
from jx_elasticsearch.es52.painless.literal import Literal
from jx_elasticsearch.es52.painless.number_op import NumberOp
from jx_elasticsearch.es52.painless.or_op import OrOp
from jx_elasticsearch.es52.painless.when_op import WhenOp
from mo_dots import Null
from mo_future import text
from mo_json import BOOLEAN, INTEGER, IS_NULL, NUMBER, STRING

MAX_INT32 = 2147483647
MIN_INT32 = -2147483648


NUMBER_TO_STRING = """
Optional.of({{expr}}).map(
    value -> {
        String output = String.valueOf(value);
        if (output.endsWith(".0")) output = output.substring(0, output.length() - 2);
        return output;
    }
).orElse(null)
"""

LIST_TO_PIPE = """
StringBuffer output=new StringBuffer();
for(String s : {{expr}}){
    output.append("|");
    String sep2="";
    StringTokenizer parts = new StringTokenizer(s, "|");
    while (parts.hasMoreTokens()){
        output.append(sep2);
        output.append(parts.nextToken());
        sep2="||";
    }//for
}//for
output.append("|");
return output.toString()
"""


def box(script):
    """
    :param es_script:
    :return: TEXT EXPRESSION WITH NON OBJECTS BOXED
    """
    if script.type is BOOLEAN:
        return "Boolean.valueOf(" + text(script.expr) + ")"
    elif script.type is INTEGER:
        return "Integer.valueOf(" + text(script.expr) + ")"
    elif script.type is NUMBER:
        return "Double.valueOf(" + text(script.expr) + ")"
    else:
        return script.expr


def _binary_to_es_script(self, schema, not_null=False, boolean=False, many=True):
    op, identity = _painless_operators[self.op]
    lhs = NumberOp(self.lhs).partial_eval().to_es_script(schema)
    rhs = NumberOp(self.rhs).partial_eval().to_es_script(schema)
    script = "(" + lhs.expr + ") " + op + " (" + rhs.expr + ")"
    missing = OrOp([self.lhs.missing(), self.rhs.missing()])

    return (
        WhenOp(
            missing,
            **{
                "then": self.default,
                "else": EsScript(type=NUMBER, expr=script, frum=self, schema=schema),
            }
        )
        .partial_eval()
        .to_es_script(schema)
    )


@extend(NullOp)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    return null_script


@extend(FalseOp_)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    return false_script


def _inequality_to_es_script(self, schema, not_null=False, boolean=False, many=True):
    op, identity = _painless_operators[self.op]
    lhs = NumberOp(self.lhs).partial_eval().to_es_script(schema).expr
    rhs = NumberOp(self.rhs).partial_eval().to_es_script(schema).expr
    script = "(" + lhs + ") " + op + " (" + rhs + ")"

    output = (
        WhenOp(
            OrOp([self.lhs.missing(), self.rhs.missing()]),
            **{
                "then": FALSE,
                "else": EsScript(type=BOOLEAN, expr=script, frum=self, schema=schema),
            }
        )
        .partial_eval()
        .to_es_script(schema)
    )
    return output


def _basic_binary_op_to_es_script(
    self, schema, not_null=False, boolean=False, many=True
):
    op, identity = _painless_operators[self.op]
    if len(self.terms) == 0:
        return Literal(identity).to_es_script(schema)
    elif len(self.terms) == 1:
        return self.terms[0].to_esscript()
    else:
        return EsScript(
            type=NUMBER,
            expr=op.join(
                "("
                + Painless[t].to_es_script(schema, not_null=True, many=False).expr
                + ")"
                for t in self.terms
            ),
            frum=self,
            schema=schema,
        )


def _multi_to_es_script(self, schema, not_null=False, boolean=False, many=True):
    op, unit = _painless_operators[self.op]
    if self.nulls:
        calc = op.join(
            "(("
            + Painless[t.missing()].to_es_script(schema).expr
            + ") ? "
            + unit
            + " : ("
            + Painless[NumberOp(t)].partial_eval().to_es_script(schema).expr
            + "))"
            for t in self.terms
        )
        return (
            WhenOp(
                AndOp([t.missing() for t in self.terms]),
                **{
                    "then": self.default,
                    "else": EsScript(type=NUMBER, expr=calc, frum=self, schema=schema),
                }
            )
            .partial_eval()
            .to_es_script(schema)
        )
    else:
        calc = op.join(
            "(" + NumberOp(t).to_es_script(schema).expr + ")" for t in self.terms
        )
        return (
            WhenOp(
                OrOp([t.missing() for t in self.terms]),
                **{
                    "then": self.default,
                    "else": EsScript(type=NUMBER, expr=calc, frum=self, schema=schema),
                }
            )
            .partial_eval()
            .to_es_script(schema)
        )


@extend(TrueOp_)
def to_es_script(self, schema, not_null=False, boolean=False, many=True):
    return true_script


Painless = Language("Painless")


_count_template = (
    "long count=0; for(v in {{expr}}) if (v!=null) count+=1; return count;"
)

_painless_operators = {
    "add": (" + ", "0"),  # (operator, zero-array default value) PAIR
    "sum": (" + ", "0"),
    "mul": (" * ", "1"),
    "basic.add": (" + ", "0"),
    "basic.mul": (" * ", "1"),
    "sub": ("-", None),
    "div": ("/", None),
    "exp": ("**", None),
    "mod": ("%", None),
    "gt": (">", None),
    "gte": (">=", None),
    "lte": ("<=", None),
    "lt": ("<", None),
}


true_script = EsScript(type=BOOLEAN, expr="true", frum=TRUE, schema=Null)
false_script = EsScript(type=BOOLEAN, expr="false", frum=FALSE, schema=Null)
null_script = EsScript(miss=TRUE, type=IS_NULL, expr="null", frum=NULL, schema=Null)
empty_string_script = EsScript(
    miss=TRUE, type=STRING, expr='""', frum=NULL, schema=Null
)
