# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Beto Dealmeida (beto@dealmeida.net)
#

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import re

from mo_future import string_types, text, first, long, is_text

from moz_sql_parser.keywords import RESERVED, reserved_keywords, join_keywords, precedence, binary_ops

VALID = re.compile(r'^[a-zA-Z_]\w*$')


def should_quote(identifier):
    """
    Return true if a given identifier should be quoted.

    This is usually true when the identifier:

      - is a reserved word
      - contain spaces
      - does not match the regex `[a-zA-Z_]\\w*`

    """
    return (
        identifier != '*' and (
            not VALID.match(identifier) or identifier in reserved_keywords))


def split_field(field):
    """
    RETURN field AS ARRAY OF DOT-SEPARATED FIELDS
    """
    if field == "." or field==None:
        return []
    elif is_text(field) and "." in field:
        if field.startswith(".."):
            remainder = field.lstrip(".")
            back = len(field) - len(remainder) - 1
            return [-1]*back + [k.replace("\a", ".") for k in remainder.replace("\\.", "\a").split(".")]
        else:
            return [k.replace("\a", ".") for k in field.replace("\\.", "\a").split(".")]
    else:
        return [field]


def join_field(path):
    """
    RETURN field SEQUENCE AS STRING
    """
    output = ".".join([f.replace(".", "\\.") for f in path if f != None])
    return output if output else "."

    # potent = [f for f in path if f != "."]
    # if not potent:
    #     return "."
    # return ".".join([f.replace(".", "\\.") for f in potent])



def escape(ident, ansi_quotes, should_quote):
    """
    Escape identifiers.

    ANSI uses single quotes, but many databases use back quotes.

    """
    def esc(identifier):
        if not should_quote(identifier):
            return identifier

        quote = '"' if ansi_quotes else '`'
        identifier = identifier.replace(quote, 2*quote)
        return '{0}{1}{2}'.format(quote, identifier, quote)
    return join_field(esc(f) for f in split_field(ident))


def Operator(op):
    prec = precedence[binary_ops[op]]
    op = ' {0} '.format(op).upper()

    def func(self, json):
        acc = []

        for v in json:
            sql = self.dispatch(v)
            if isinstance(v, (text, int, float, long)):
                acc.append(sql)
                continue

            p = precedence.get(first(v.keys()))
            if p is None:
                acc.append(sql)
                continue
            if p>=prec:
                acc.append("(" + sql + ")")
            else:
                acc.append(sql)
        return op.join(acc)
    return func


class Formatter:

    clauses = [
        'with_',
        'select',
        'from_',
        'where',
        'groupby',
        'having',
        'orderby',
        'limit',
        'offset',
    ]

    # simple operators
    _concat = Operator('||')
    _mul = Operator('*')
    _div = Operator('/')
    _mod = Operator('%')
    _add = Operator('+')
    _sub = Operator('-')
    _neq = Operator('<>')
    _gt = Operator('>')
    _lt = Operator('<')
    _gte = Operator('>=')
    _lte = Operator('<=')
    _eq = Operator('=')
    _or = Operator('or')
    _and = Operator('and')
    _binary_and = Operator("&")
    _binary_or = Operator("|")

    def __init__(self, ansi_quotes=True, should_quote=should_quote):
        self.ansi_quotes = ansi_quotes
        self.should_quote = should_quote

    def format(self, json):
        if 'union' in json:
            return self.union(json['union'])
        elif 'union_all' in json:
            return self.union_all(json['union_all'])
        else:
            return self.query(json)

    def dispatch(self, json):
        if isinstance(json, list):
            return self.delimited_list(json)
        if isinstance(json, dict):
            if len(json) == 0:
                return ''
            elif 'value' in json:
                return self.value(json)
            elif 'from' in json:
                # Nested queries
                return '({})'.format(self.format(json))
            elif 'select' in json:
                # Nested queries
                return '({})'.format(self.format(json))
            else:
                return self.op(json)
        if isinstance(json, string_types):
            return escape(json, self.ansi_quotes, self.should_quote)

        return text(json)

    def delimited_list(self, json):
        return ', '.join(self.dispatch(element) for element in json)

    def value(self, json):
        parts = [self.dispatch(json['value'])]
        if 'name' in json:
            parts.extend(['AS', self.dispatch(json['name'])])
        return ' '.join(parts)

    def op(self, json):
        if 'on' in json:
            return self._on(json)

        if len(json) > 1:
            raise Exception('Operators should have only one key!')
        key, value = list(json.items())[0]

        # check if the attribute exists, and call the corresponding method;
        # note that we disallow keys that start with `_` to avoid giving access
        # to magic methods
        attr = '_{0}'.format(key)
        if hasattr(self, attr) and not key.startswith('_'):
            method = getattr(self, attr)
            return method(value)

        # treat as regular function call
        if isinstance(value, dict) and len(value) == 0:
            return key.upper() + "()"  # NOT SURE IF AN EMPTY dict SHOULD BE DELT WITH HERE, OR IN self.dispatch()
        else:
            return '{0}({1})'.format(key.upper(), self.dispatch(value))

    def _binary_not(self, value):
        return '~{0}'.format(self.dispatch(value))

    def _exists(self, value):
        return '{0} IS NOT NULL'.format(self.dispatch(value))

    def _missing(self, value):
        return '{0} IS NULL'.format(self.dispatch(value))

    def _like(self, pair):
        return '{0} LIKE {1}'.format(self.dispatch(pair[0]), self.dispatch(pair[1]))

    def _nlike(self, pair):
        return '{0} NOT LIKE {1}'.format(self.dispatch(pair[0]), self.dispatch(pair[1]))

    def _is(self, pair):
        return '{0} IS {1}'.format(self.dispatch(pair[0]), self.dispatch(pair[1]))

    def _in(self, json):
        valid = self.dispatch(json[1])
        # `(10, 11, 12)` does not get parsed as literal, so it's formatted as
        # `10, 11, 12`. This fixes it.
        if not valid.startswith('('):
            valid = '({0})'.format(valid)

        return '{0} IN {1}'.format(json[0], valid)

    def _nin(self, json):
        valid = self.dispatch(json[1])
        # `(10, 11, 12)` does not get parsed as literal, so it's formatted as
        # `10, 11, 12`. This fixes it.
        if not valid.startswith('('):
            valid = '({0})'.format(valid)

        return '{0} NOT IN {1}'.format(json[0], valid)

    def _case(self, checks):
        parts = ['CASE']
        for check in checks:
            if isinstance(check, dict):
                if 'when' in check and 'then' in check:
                    parts.extend(['WHEN', self.dispatch(check['when'])])
                    parts.extend(['THEN', self.dispatch(check['then'])])
                else:
                    parts.extend(['ELSE', self.dispatch(check)])
            else:
                parts.extend(['ELSE', self.dispatch(check)])
        parts.append('END')
        return ' '.join(parts)

    def _literal(self, json):
        if isinstance(json, list):
            return '({0})'.format(', '.join(self._literal(v) for v in json))
        elif isinstance(json, string_types):
            return "'{0}'".format(json.replace("'", "''"))
        else:
            return str(json)

    def _between(self, json):
        return '{0} BETWEEN {1} AND {2}'.format(self.dispatch(json[0]), self.dispatch(json[1]), self.dispatch(json[2]))

    def _not_between(self, json):
        return '{0} NOT BETWEEN {1} AND {2}'.format(self.dispatch(json[0]), self.dispatch(json[1]), self.dispatch(json[2]))

    def _on(self, json):
        detected_join = join_keywords & set(json.keys())
        if len(detected_join) == 0:
            raise Exception(
                'Fail to detect join type! Detected: "{}" Except one of: "{}"'.format(
                    [on_keyword for on_keyword in json if on_keyword != 'on'][0],
                    '", "'.join(join_keywords)
                )
            )

        join_keyword = detected_join.pop()

        return '{0} {1} ON {2}'.format(
            join_keyword.upper(), self.dispatch(json[join_keyword]), self.dispatch(json['on'])
        )

    def union(self, json):
        return ' UNION '.join(self.query(query) for query in json)

    def union_all(self, json):
        return ' UNION ALL '.join(self.query(query) for query in json)

    def query(self, json):
        return ' '.join(
            part
            for clause in self.clauses
            for part in [getattr(self, clause)(json)]
            if part
        )

    def with_(self, json):
        if 'with' in json:
            with_ = json['with']
            if not isinstance(with_, list):
                with_ = [with_]
            parts = ', '.join(
                '{0} AS {1}'.format(part['name'], self.dispatch(part['value']))
                for part in with_
            )
            return 'WITH {0}'.format(parts)

    def select(self, json):
        if 'select' in json:
            return 'SELECT {0}'.format(self.dispatch(json['select']))

    def from_(self, json):
        is_join = False
        if 'from' in json:
            from_ = json['from']
            if 'union' in from_:
                return self.union(from_['union'])
            if not isinstance(from_, list):
                from_ = [from_]

            parts = []
            for token in from_:
                if join_keywords & set(token):
                    is_join = True
                parts.append(self.dispatch(token))
            joiner = ' ' if is_join else ', '
            rest = joiner.join(parts)
            return 'FROM {0}'.format(rest)

    def where(self, json):
        if 'where' in json:
            return 'WHERE {0}'.format(self.dispatch(json['where']))

    def groupby(self, json):
        if 'groupby' in json:
            return 'GROUP BY {0}'.format(self.dispatch(json['groupby']))

    def having(self, json):
        if 'having' in json:
            return 'HAVING {0}'.format(self.dispatch(json['having']))

    def orderby(self, json):
        if 'orderby' in json:
            orderby = json['orderby']
            if isinstance(orderby, dict):
                orderby = [orderby]
            return 'ORDER BY {0}'.format(','.join([
                '{0} {1}'.format(self.dispatch(o), o.get('sort', '').upper()).strip()
                for o in orderby
            ]))

    def limit(self, json):
        if 'limit' in json:
            if json['limit']:
                return 'LIMIT {0}'.format(self.dispatch(json['limit']))

    def offset(self, json):
        if 'offset' in json:
            return 'OFFSET {0}'.format(self.dispatch(json['offset']))
