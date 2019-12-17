# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

"""
# NOTE:

THE self.lang[operator] PATTERN IS CASTING NEW OPERATORS TO OWN LANGUAGE;
KEEPING Python AS# Python, ES FILTERS AS ES FILTERS, AND Painless AS
Painless. WE COULD COPY partial_eval(), AND OTHERS, TO THIER RESPECTIVE
LANGUAGE, BUT WE KEEP CODE HERE SO THERE IS LESS OF IT

"""
from __future__ import absolute_import, division, unicode_literals

import operator

from jx_base.expressions.abs_op import AbsOp
from jx_base.expressions.add_op import AddOp
from jx_base.expressions.and_op import AndOp
from jx_base.expressions.basic_add_op import BasicAddOp
from jx_base.expressions.basic_mul_op import BasicMulOp
from jx_base.expressions.between_op import BetweenOp
from jx_base.expressions.case_op import CaseOp
from jx_base.expressions.coalesce_op import CoalesceOp
from jx_base.expressions.concat_op import ConcatOp
from jx_base.expressions.count_op import CountOp
from jx_base.expressions.date_op import DateOp
from jx_base.expressions.div_op import DivOp
from jx_base.expressions.eq_op import EqOp
from jx_base.expressions.exists_op import ExistsOp
from jx_base.expressions.exp_op import ExpOp
from jx_base.expressions.false_op import FalseOp
from jx_base.expressions.find_op import FindOp
from jx_base.expressions.first_op import FirstOp
from jx_base.expressions.floor_op import FloorOp
from jx_base.expressions.from_unix_op import FromUnixOp
from jx_base.expressions.get_op import GetOp
from jx_base.expressions.gt_op import GtOp
from jx_base.expressions.gte_op import GteOp
from jx_base.expressions.in_op import InOp
from jx_base.expressions.is_number_op import IsNumberOp
from jx_base.expressions.is_string_op import IsStringOp
from jx_base.expressions.last_op import LastOp
from jx_base.expressions.left_op import LeftOp
from jx_base.expressions.length_op import LengthOp
from jx_base.expressions.literal import Literal
from jx_base.expressions.lt_op import LtOp
from jx_base.expressions.lte_op import LteOp
from jx_base.expressions.max_op import MaxOp
from jx_base.expressions.missing_op import MissingOp
from jx_base.expressions.mod_op import ModOp
from jx_base.expressions.mul_op import MulOp
from jx_base.expressions.ne_op import NeOp
from jx_base.expressions.not_left_op import NotLeftOp
from jx_base.expressions.not_op import NotOp
from jx_base.expressions.not_right_op import NotRightOp
from jx_base.expressions.null_op import NullOp
from jx_base.expressions.number_op import NumberOp
from jx_base.expressions.offset_op import OffsetOp
from jx_base.expressions.or_op import OrOp
from jx_base.expressions.prefix_op import PrefixOp
from jx_base.expressions.range_op import RangeOp
from jx_base.expressions.reg_exp_op import RegExpOp
from jx_base.expressions.right_op import RightOp
from jx_base.expressions.rows_op import RowsOp
from jx_base.expressions.script_op import ScriptOp
from jx_base.expressions.select_op import SelectOp
from jx_base.expressions.split_op import SplitOp
from jx_base.expressions.string_op import StringOp
from jx_base.expressions.sub_op import SubOp
from jx_base.expressions.suffix_op import SuffixOp
from jx_base.expressions.true_op import TrueOp
from jx_base.expressions.tuple_op import TupleOp
from jx_base.expressions.union_op import UnionOp
from jx_base.expressions.unix_op import UnixOp
from jx_base.expressions.variable import Variable
from jx_base.expressions.when_op import WhenOp
from jx_base.language import TYPE_ORDER, define_language, is_expression
from mo_dots import Null, is_sequence, FlatList
from mo_future import (
    first,
    get_function_name,
    is_text,
    items as items_,
    text,
    utf8_json_encoder,
)
from mo_json import BOOLEAN, INTEGER, IS_NULL, NUMBER, OBJECT, STRING, scrub
from mo_logs import Except, Log
from mo_math import is_number

ALLOW_SCRIPTING = False
EMPTY_DICT = {}


def extend(cls):
    """
    DECORATOR TO ADD METHODS TO CLASSES
    :param cls: THE CLASS TO ADD THE METHOD TO
    :return:
    """

    def extender(func):
        setattr(cls, get_function_name(func), func)
        return func

    return extender


def last(values):
    if len(values):
        if isinstance(values, FlatList):
            return values.last()
        if is_sequence(values):
            return values[-1]
        else:
            return first(values)
    else:
        return Null


def simplified(func):
    def mark_as_simple(self):
        if self.simplified:
            return self

        output = func(self)
        output.simplified = True
        return output

    func_name = get_function_name(func)
    mark_as_simple.__name__ = func_name
    return mark_as_simple


def jx_expression(expr, schema=None):
    if expr == None:
        return Null

    # UPDATE THE VARIABLE WITH THIER KNOWN TYPES
    if not schema:
        output = _jx_expression(expr, language)
        return output
    output = _jx_expression(expr, language)
    for v in output.vars():
        leaves = schema.leaves(v.var)
        if len(leaves) == 0:
            v.data_type = IS_NULL
        if len(leaves) == 1:
            v.data_type = first(leaves).jx_type
    return output


def _jx_expression(expr, lang):
    """
    WRAP A JSON EXPRESSION WITH OBJECT REPRESENTATION
    """
    if is_expression(expr):
        # CONVERT TO lang
        new_op = lang[expr]
        if not new_op:
            # CAN NOT BE FOUND, TRY SOME PARTIAL EVAL
            return language[expr.get_id()].partial_eval()
        return expr
        # return new_op(expr.args)  # THIS CAN BE DONE, BUT IT NEEDS MORE CODING, AND I WOULD EXPECT IT TO BE SLOW

    if expr is None:
        return TRUE
    elif is_text(expr):
        return Variable(expr)
    elif expr in (True, False, None) or expr == None or is_number(expr):
        return Literal(expr)
    elif expr.__class__ is Date:
        return Literal(expr.unix)
    elif is_sequence(expr):
        return lang[TupleOp([_jx_expression(e, lang) for e in expr])]

    # expr = wrap(expr)
    try:
        items = items_(expr)

        for op, term in items:
            # ONE OF THESE IS THE OPERATOR
            full_op = operators.get(op)
            if full_op:
                class_ = lang.ops[full_op.get_id()]
                if class_:
                    return class_.define(expr)

                # THIS LANGUAGE DOES NOT SUPPORT THIS OPERATOR, GOTO BASE LANGUAGE AND GET THE MACRO
                class_ = language[op.get_id()]
                output = class_.define(expr).partial_eval()
                return _jx_expression(output, lang)
        else:
            if not items:
                return NULL
            raise Log.error("{{instruction|json}} is not known", instruction=expr)

    except Exception as e:
        Log.error("programmer error expr = {{value|quote}}", value=expr, cause=e)


IDENTITY = Variable(".")


_json_encoder = utf8_json_encoder


def value2json(value):
    try:
        scrubbed = scrub(value, scrub_number=float)
        return text(_json_encoder(scrubbed))
    except Exception as e:
        e = Except.wrap(e)
        Log.warning("problem serializing {{type}}", type=text(repr(value)), cause=e)
        raise e


literal_op_ids = (
    Literal.get_id(),
    NullOp.get_id(),
    TrueOp.get_id(),
    FalseOp.get_id(),
    DateOp.get_id(),
)


def is_literal(l):
    try:
        return l.get_id() in literal_op_ids
    except Exception:
        return False


###############################################################################
## THE Basic OPERTATIONS ARE null-UNAWARE OPERATIONS
###############################################################################

###############################################################################
##  Sql OPERATORS ARE CONSERVATIVE null OPERATORS
###############################################################################

language = define_language(None, vars())


def merge_types(jx_types):
    """
    :param jx_types: ITERABLE OF jx TYPES
    :return: ONE TYPE TO RULE THEM ALL
    """
    return _merge_types[max(_merge_score[t] for t in jx_types)]


_merge_score = {IS_NULL: 0, BOOLEAN: 1, INTEGER: 2, NUMBER: 3, STRING: 4, OBJECT: 5}
_merge_types = {v: k for k, v in _merge_score.items()}

builtin_ops = {
    "ne": operator.ne,
    "eq": operator.eq,
    "gte": operator.ge,
    "gt": operator.gt,
    "lte": operator.le,
    "lt": operator.lt,
    "add": operator.add,
    "sub": operator.sub,
    "mul": operator.mul,
    "max": lambda *v: max(v),
    "min": lambda *v: min(v),
}

operators = {
    "abs": AbsOp,
    "add": AddOp,
    "and": AndOp,
    "basic.add": BasicAddOp,
    "basic.mul": BasicMulOp,
    "between": BetweenOp,
    "case": CaseOp,
    "coalesce": CoalesceOp,
    "concat": ConcatOp,
    "count": CountOp,
    "date": DateOp,
    "div": DivOp,
    "divide": DivOp,
    "eq": EqOp,
    "exists": ExistsOp,
    "exp": ExpOp,
    "find": FindOp,
    "first": FirstOp,
    "floor": FloorOp,
    "from_unix": FromUnixOp,
    "get": GetOp,
    "gt": GtOp,
    "gte": GteOp,
    "in": InOp,
    "instr": FindOp,
    "is_number": IsNumberOp,
    "is_string": IsStringOp,
    "last": LastOp,
    "left": LeftOp,
    "length": LengthOp,
    "literal": Literal,
    "lt": LtOp,
    "lte": LteOp,
    "match_all": TrueOp,
    "max": MaxOp,
    "minus": SubOp,
    "missing": MissingOp,
    "mod": ModOp,
    "mul": MulOp,
    "mult": MulOp,
    "multiply": MulOp,
    "ne": NeOp,
    "neq": NeOp,
    "not": NotOp,
    "not_left": NotLeftOp,
    "not_right": NotRightOp,
    "null": NullOp,
    "number": NumberOp,
    "offset": OffsetOp,
    "or": OrOp,
    "postfix": SuffixOp,
    "prefix": PrefixOp,
    "range": RangeOp,
    "regex": RegExpOp,
    "regexp": RegExpOp,
    "right": RightOp,
    "rows": RowsOp,
    "script": ScriptOp,
    "select": SelectOp,
    "split": SplitOp,
    "string": StringOp,
    "suffix": SuffixOp,
    "sub": SubOp,
    "subtract": SubOp,
    "sum": AddOp,
    "term": EqOp,
    "terms": InOp,
    "tuple": TupleOp,
    "union": UnionOp,
    "unix": UnixOp,
    "when": WhenOp,
}
